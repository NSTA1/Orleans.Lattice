using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Lattice.Primitives;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The leaf's side of the WAL append fence (epic #5605): every append the leaf
/// makes goes through its activation's <see cref="LeafAppendEpochFence"/>, which
/// claims the activation's epoch at each partition and stamps the append while the
/// fence gate is open. This member only fences: nothing here lets the leaf's
/// checkpoint skip a record it did not read.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>This activation's append fence, created with the commit-log writer.</summary>
    private LeafAppendEpochFence? _appendFence;

    /// <summary>
    /// The fence a bulk seed in flight on this call appends through instead of the
    /// activation's own (#5812), so seeding claims nothing. Flows with the seeding call
    /// only: an interleaved call on the same activation never sees it.
    /// </summary>
    private static readonly AsyncLocal<ILeafAppendFence?> BulkSeedScope = new();

    /// <inheritdoc />
    public async Task MergeBulkSeedAsync(Dictionary<string, LwwValue<byte[]>> entries, WalRoutingStamp seedRouting)
    {
        var previous = BulkSeedScope.Value;
        // RED PROOF (#5812 B): the seed runs outside its scope, so it claims like a live write.
        try
        {
            await MergeEntriesAsync(entries);
        }
        finally
        {
            BulkSeedScope.Value = previous;
        }
    }

    /// <summary>This activation's append fence, or <see langword="null"/> when it has none; test seam.</summary>
    internal LeafAppendEpochFence? AppendFenceForTesting => _appendFence;

    /// <summary>
    /// Wraps <paramref name="inner"/> in this activation's append fence when the
    /// writer supports fencing and the silo provides the fence gate and the tree's
    /// grain storage; otherwise returns it unchanged, so the leaf appends unstamped.
    /// </summary>
    private ICommitLogWriter WithAppendFence(ICommitLogWriter inner)
    {
        if (inner is not IFencedCommitLogWriter fenced
            || context.ActivationServices is not { } services
            || services.GetService<IWalAppendFenceGate>() is not { } gate
            || WalAppendFenceStorage.Resolve(services) is not { } storage)
        {
            return inner;
        }

        _appendFence = new LeafAppendEpochFence(
            context.GrainId.GetGuidKey(),
            gate,
            storage,
            OnAppendEpochSuperseded,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, MetricTreeId),
            LatticeTenantLabel.ForTree(state.State.TreeId ?? string.Empty),
            requireReachabilityProof: true);
        return new FencedCommitLogWriter(fenced, _appendFence);
    }

    /// <summary>The proof in flight, shared by concurrent first appends.</summary>
    private Task? _stampReachabilityProof;

    /// <summary>
    /// Proves, once per activation and before its first append-epoch claim, that this
    /// leaf is reachable by descent (#5709). An unreachable leaf never appends stamped:
    /// its appends go unstamped and are reported to every leaf sharing the partition, so
    /// a reachable leaf that owns the same keys replays them before it can arm. A leaf
    /// with a hand-off marker open on it - a split sibling the parent has not yet learned
    /// of - is exempt: it admits only keys its donor has narrowed away. Completes
    /// synchronously once decided, so it costs an append one flag read.
    /// </summary>
    internal ValueTask ProveStampReachabilityAsync()
    {
        if (_appendFence is not { } fence || !fence.NeedsReachabilityProof)
        {
            return default;
        }

        if (BulkSeedScope.Value is not null)
        {
            // #5812: a bulk seed claims nothing, so it needs no proof, and its leaf is
            // not yet published: proving now would fail and latch the leaf unstamped.
            // The leaf's first live write, once it is reachable, proves instead.
            return default;
        }

        if (_stampReachabilityProof is { IsCompleted: false } inFlight)
        {
            return new ValueTask(inFlight);
        }

        var proof = ProveStampReachabilityCoreAsync(fence);
        _stampReachabilityProof = proof.IsCompleted ? null : proof;
        return new ValueTask(proof);
    }

    private async Task ProveStampReachabilityCoreAsync(LeafAppendEpochFence fence)
    {
        if (state.State.TreeId is not { Length: > 0 } treeId || state.State.ShardIndex is not { } shardIndex)
        {
            // No shard root to ask: the leaf cannot prove it, so it appends unstamped.
            fence.OnReachabilityUndecided();
            return;
        }

        try
        {
            if (await LiveAdvanceShardRoot(treeId, shardIndex).IsReachableByDescentAsync(context.GrainId, state.State.LowKeyInclusive))
            {
                fence.OnReachabilityProven();
                return;
            }

            if (HandOffMarkerStorage() is { } storage
                && (await HandOffMarkerRows.ReadLeafAsync(storage, context.GrainId.GetGuidKey())).State.Open.Count > 0)
            {
                fence.OnReachabilityProven();
                return;
            }

            StopStampingUnreachable(fence);
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogDebug(
                ex,
                "Leaf {LeafId}: could not prove it is reachable by descent; it appends unstamped and tries again after a backoff (epic #5605, #5709).",
                context.GrainId);
            fence.OnReachabilityUndecided();
        }
    }

    /// <summary>Latches <paramref name="fence"/> unstamped for the activation, logging once.</summary>
    private void StopStampingUnreachable(LeafAppendEpochFence fence)
    {
        if (fence.StopStampingUnreachable())
        {
            ResolveLogger()?.LogWarning(
                "Leaf {LeafId} of tree {TreeId} is not reachable by descent and no hand-off is open on it: it appends unstamped for the rest of this activation, so the leaf that owns its keys replays them (epic #5605, #5709). An orphaned leaf; see orphaned-leaf repair.",
                context.GrainId,
                MetricTreeId);
        }
    }

    /// <summary>
    /// Called once when another activation of this leaf has claimed a newer append
    /// epoch: this activation is a stale duplicate. It stops appending and
    /// deactivates, and every write it refused is retried by its caller's seam
    /// against the live activation.
    /// </summary>
    private void OnAppendEpochSuperseded(long epoch)
    {
        ResolveLogger()?.LogWarning(
            "Leaf {LeafId} of tree {TreeId}: this activation's WAL append epoch {Epoch} has been superseded by another activation of the same leaf. It stops appending and deactivates; its callers retry against the live activation (epic #5605).",
            context.GrainId,
            MetricTreeId,
            epoch);
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested,
            "The leaf's WAL append epoch was superseded by another activation."));
    }

    /// <summary>
    /// Retires this leaf's append-epoch entries at every WAL partition of
    /// <paramref name="treeId"/>, then deletes its epoch row (epic #5605). Runs on
    /// every path that clears the leaf, whether or not the fence gate is open. A
    /// leaf with no epoch row never claimed anywhere, so it costs one storage read.
    /// </summary>
    /// <remarks>
    /// The epoch row is deleted only once every partition has retired its entry,
    /// and conditionally on the ETag read, so a row that moved since the read is
    /// kept: a re-created incarnation's claim, or a pre-clear duplicate's bump above
    /// a floor this clear raised. Each partition floors at the row value read
    /// (#5641). A failed retirement is logged and leaves the rows in place, which
    /// is safe.
    /// </remarks>
    private async Task RetireAppendEpochAsync(string? treeId)
    {
        if (string.IsNullOrEmpty(treeId)
            || WalAppendFenceStorage.Resolve(context.ActivationServices) is not { } storage)
        {
            return;
        }

        try
        {
            // The conditional row delete may conflict, when the row moved since the
            // read: another claim moved it, a re-created incarnation or a pre-clear
            // duplicate's bump, so it is kept (#5605 re-review of revision 7, N1).
            await WalAppendFenceRetirement.RetireLeafAsync(
                grainFactory,
                storage,
                treeId,
                () => optionsResolver.GetWalPartitionsAsync(treeId),
                context.GrainId.GetGuidKey(),
                Array.Empty<int>()).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            // Best effort, and safe to leave: the epoch row is kept, so a leaf
            // re-created under this id continues above every entry still standing,
            // and a leftover entry only ever sits above a re-claim.
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {LeafId} of tree {TreeId}: could not retire its WAL append-epoch entries; the rows are left in place, which is safe (epic #5605).",
                context.GrainId,
                treeId);
        }
        finally
        {
            // The leaf is cleared: this activation, if it keeps serving, claims afresh
            // as the re-created incarnation rather than meeting its own retired
            // entries and deactivating (#5605 review, N1).
            _appendFence?.ResetAfterClear();
        }
    }

    /// <summary>
    /// The commit-log writer the leaf appends through: every append is stamped by
    /// this activation's fence (epic #5605).
    /// </summary>
    private sealed class FencedCommitLogWriter(IFencedCommitLogWriter inner, ILeafAppendFence fence) : IRoutingStampedCommitLogWriter
    {
        /// <inheritdoc />
        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default) =>
            inner.AppendAsync(entry, BulkSeedScope.Value ?? fence, cancellationToken);

        /// <inheritdoc />
        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) =>
            inner.AppendManyAsync(entries, BulkSeedScope.Value ?? fence, cancellationToken);

        /// <inheritdoc />
        public Task<long> AppendAsync(WalRecord entry, WalRoutingStamp routing, CancellationToken cancellationToken) =>
            inner.AppendAsync(entry, fence, routing, cancellationToken);

        /// <inheritdoc />
        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, WalRoutingStamp routing, CancellationToken cancellationToken) =>
            inner.AppendManyAsync(entries, fence, routing, cancellationToken);
    }
}
