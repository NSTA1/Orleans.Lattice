using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Lattice.BPlusTree.State;
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
    /// The sibling id of the last narrow this activation durably wrote, or empty (#5837,
    /// X1, X3, X7). Held here as well as on the fence, which is created lazily with the
    /// commit-log writer and so may not exist yet when the narrow persists.
    /// </summary>
    private Guid _persistedNarrowTag;

    /// <summary>Records this activation's narrow to <paramref name="siblingId"/> as durably written (#5837, X7).</summary>
    private void NotePersistedNarrow(Guid siblingId)
    {
        _persistedNarrowTag = siblingId;
        _appendFence?.NotePersistedNarrow(siblingId);
    }

    /// <summary>Withdraws the persisted narrow tag on an ETag conflict on the leaf's state (#5837, X7).</summary>
    private void WithdrawPersistedNarrow()
    {
        _persistedNarrowTag = Guid.Empty;
        _appendFence?.WithdrawNarrow();
    }

    /// <summary>
    /// The fence a bulk seed in flight on this call appends through instead of the
    /// activation's own (#5812), so seeding claims nothing. Flows with the seeding call
    /// only: an interleaved call on the same activation never sees it.
    /// </summary>
    private static readonly AsyncLocal<ILeafAppendFence?> BulkSeedScope = new();

    /// <summary>
    /// Ends the bulk-seed scope for the work that calls it (review S2 of #5830). The scope
    /// is an <see cref="AsyncLocal{T}"/>, so detached work started inside a seed's
    /// <see cref="MergeEntriesAsync"/> would inherit it and append through the seed's
    /// fence, unproved. Called first by every such task the leaf starts: the replay a
    /// seed's barrier wait can re-arm, whose replayed terminals append. Set inside an
    /// async method, the change stays in that method's flow. Reads one field when no
    /// scope is set.
    /// </summary>
    private static void LeaveBulkSeedScope()
    {
        if (BulkSeedScope.Value is not null)
        {
            BulkSeedScope.Value = null;
        }
    }

    /// <summary>Whether the calling flow is inside a bulk seed's scope; test seam.</summary>
    internal static bool InBulkSeedScopeForTesting => BulkSeedScope.Value is not null;

    /// <inheritdoc />
    public async Task MergeBulkSeedAsync(Dictionary<string, LwwValue<byte[]>> entries, WalRoutingStamp seedRouting)
    {
        var previous = BulkSeedScope.Value;
        BulkSeedScope.Value = new BulkSeedAppendFence(context.GrainId.GetGuidKey(), seedRouting, _appendFence);
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
            requireReachabilityProof: true,
            loadedStateCurrent: new LeafStateCurrencyProbe(storage, context.GrainId, () => state.Etag, _persistWriteGate).IsCurrentAsync);
        if (_persistedNarrowTag != Guid.Empty)
        {
            _appendFence.NotePersistedNarrow(_persistedNarrowTag);
        }

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
            var verdict = await LiveAdvanceShardRoot(treeId, shardIndex).GetDescentVerdictAsync(context.GrainId, state.State.LowKeyInclusive);
            if (verdict == DescentVerdict.Undecided && false) // RED-PROOF MD2-proof: undecided read as unreachable
            {
                // #5876, D2: the descent overlapped a routing-structure mutation, so it is no
                // verdict; the leaf appends unstamped for a backoff and asks again (R1).
                fence.OnReachabilityUndecided();
                return;
            }

            if (verdict == DescentVerdict.Reachable)
            {
                // a5e2e2ef's G2 and G3 on #5866: reachable means linked, not witnessed. A
                // deferred activation, and any proof under the eager gate (which may claim),
                // reads the marker first: while a donor bound is pending it stays deferred
                // (pre-arm) or undecided (eager), never claiming before its own link is
                // witnessed. Under the pre-arm gate a first proof needs no read: it cannot
                // claim, and it is re-made with the marker before an eager claim.
                var checkMarker = fence.IsDeferred || !IsPreArmGateOpen();
                if (checkMarker && HandOffMarkerStorage() is { } reachedStorage)
                {
                    var reachedMarker = (await HandOffMarkerRows.ReadLeafAsync(reachedStorage, context.GrainId.GetGuidKey())).State;
                    if (HoldsPendingDonorBound(reachedMarker))
                    {
                        if (IsPreArmGateOpen())
                        {
                            fence.OnReachabilityDeferred();
                        }
                        else
                        {
                            fence.OnReachabilityUndecided();
                        }

                        return;
                    }

                    fence.OnReachabilityProven(markerChecked: true);
                    return;
                }

                fence.OnReachabilityProven(markerChecked: HandOffMarkerStorage() is null);
                return;
            }

            var orphanRepair = false;
            if (HandOffMarkerStorage() is { } storage)
            {
                var marker = (await HandOffMarkerRows.ReadLeafAsync(storage, context.GrainId.GetGuidKey())).State;
                if (marker.Open.Count > 0)
                {
                    fence.OnReachabilityProven(markerChecked: true);
                    return;
                }

                if (IsPreArmGateOpen() && marker.Closes.Count > 0)
                {
                    // #5837, the latch exemption: a sibling its donor closed before the
                    // link is deferred, never proven, while the bound is pending and no
                    // orphan repair runs; orphan repair always latches.
                    var view = await LiveAdvanceShardRoot(treeId, shardIndex)
                        .GetLiveAdvanceViewAsync(context.GrainId, state.State.LowKeyInclusive, state.State.HighKeyExclusive);
                    if (view.Reachable && !view.OrphanRepairInProgress)
                    {
                        // G2: linked is not witnessed; a pending bound keeps it deferred.
                        if (HoldsPendingDonorBound(marker))
                        {
                            fence.OnReachabilityDeferred();
                        }
                        else
                        {
                            fence.OnReachabilityProven(markerChecked: true);
                        }

                        return;
                    }

                    if (!view.OrphanRepairInProgress && HoldsPendingDonorBound(marker))
                    {
                        // a5e2e2ef's D1-D4 (#5851): deferred, it stamps pre-arm rather than
                        // appending unstamped for a backoff, so a pre-link sibling never
                        // raises the shared watermark; it still never claims or arms.
                        fence.OnReachabilityDeferred();
                        return;
                    }

                    orphanRepair = view.OrphanRepairInProgress;
                }
            }

            OnUnreachableVerdict(fence, orphanRepair);
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

    /// <summary>
    /// Marks this activation proven reachable at its split birth (#5837, P1-P3). The fence
    /// is created lazily with the commit-log writer, so the writer is resolved first.
    /// </summary>
    private void ProveReachableAtSplitBirth()
    {
        _ = ResolveCommitLogWriter();
        _appendFence?.OnReachabilityProven();
    }

    /// <summary>
    /// Whether <paramref name="marker"/> holds a split donor's bound whose sibling's own
    /// link is not yet witnessed (a5e2e2ef's B2 on #5866): while it does, under the pre-arm
    /// gate and with no orphan repair running, an unreachable leaf is a split sibling not
    /// yet linked, whose reachability decision is deferred, never latched (the latch
    /// exemption). Once the link is witnessed, an unreachable leaf latches (#5709).
    /// </summary>
    private bool HoldsPendingDonorBound(LeafHandOffState marker)
    {
        var pending = _pendingDonorClosesScratch ??= new List<WalPendingClose>(WalAppendStamp.MaxPendingCloses);
        pending.Clear();
        return HandOffMarkerCore.CollectPendingDonorBounds(
            marker, state.State.LowKeyInclusive, state.State.HighKeyExclusive, pending) > 0;
    }

    /// <summary>
    /// Acts on one verdict that descent does not reach this activation and no hand-off is
    /// open on it (#5872, a5e2e2ef's R1-R4), from the proof path or the view path alike. A
    /// structural witness - orphan repair running, the leaf absorbed by a reclaim, or this
    /// activation superseded - latches at once; otherwise the first verdict leaves it
    /// undecided, appending unstamped for the backoff, and only a second one after it
    /// latches. Costs the write path nothing: a suspected latch costs one more proof read.
    /// </summary>
    private void OnUnreachableVerdict(LeafAppendEpochFence fence, bool orphanRepair)
    {
        if (orphanRepair || state.State.ReclaimRetired || _reclaimRetired != 0 || fence.IsStale)
        {
            StopStampingUnreachable(fence);
            return;
        }

        if (fence.StopStampingUnreachable() || fence.OnUnreachableVerdict()) // RED-PROOF R1a: one verdict latches
        {
            LogStoppedStampingUnreachable();
            return;
        }

        ResolveLogger()?.LogDebug(
            "Leaf {LeafId}: descent did not reach it; it appends unstamped for a backoff and proves again before it latches (#5872).",
            context.GrainId);
    }

    /// <summary>Latches <paramref name="fence"/> unstamped for the activation, logging once.</summary>
    private void StopStampingUnreachable(LeafAppendEpochFence fence)
    {
        if (fence.StopStampingUnreachable())
        {
            LogStoppedStampingUnreachable();
        }
    }

    private void LogStoppedStampingUnreachable() =>
        ResolveLogger()?.LogWarning(
            "Leaf {LeafId} of tree {TreeId} is not reachable by descent and no hand-off is open on it: it appends unstamped for the rest of this activation, so the leaf that owns its keys replays them (epic #5605, #5709). An orphaned leaf; see orphaned-leaf repair.",
            context.GrainId,
            MetricTreeId);

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
