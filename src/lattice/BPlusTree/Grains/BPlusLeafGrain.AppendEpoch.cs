using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
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
            LatticeTenantLabel.ForTree(state.State.TreeId ?? string.Empty));
        return new FencedCommitLogWriter(fenced, _appendFence);
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
    /// and conditionally on the ETag read, so a leaf re-created under the same id
    /// that has claimed since keeps its row; its own entries then sit above every
    /// floor this retirement raised. A failed retirement is logged and leaves the
    /// rows in place, which is safe.
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
            var leafId = context.GrainId.GetGuidKey();
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, leafId).ConfigureAwait(true);
            if (!row.RecordExists)
            {
                return;
            }

            var partitions = await optionsResolver.GetWalPartitionsAsync(treeId);
            var retirements = new Task[partitions];
            for (var partition = 0; partition < partitions; partition++)
            {
                retirements[partition] = grainFactory
                    .GetGrain<IWalShardGrain>($"{treeId}/{partition}")
                    .RetireAppendEpochAsync(leafId, CancellationToken.None);
            }

            // The epoch row goes only once every partition has retired its entry.
            await Task.WhenAll(retirements);
            try
            {
                await WalAppendFenceRows.ClearAsync(
                    storage,
                    WalAppendFenceRows.LeafEpochGrainType,
                    WalAppendFenceRows.LeafEpochKey(leafId),
                    row).ConfigureAwait(true);
            }
            catch (InconsistentStateException)
            {
                // Claimed again since the read: a re-created incarnation owns the row now.
            }
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
    }

    /// <summary>
    /// The commit-log writer the leaf appends through: every append is stamped by
    /// this activation's fence (epic #5605).
    /// </summary>
    private sealed class FencedCommitLogWriter(IFencedCommitLogWriter inner, ILeafAppendFence fence) : ICommitLogWriter
    {
        /// <inheritdoc />
        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default) =>
            inner.AppendAsync(entry, fence, cancellationToken);

        /// <inheritdoc />
        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) =>
            inner.AppendManyAsync(entries, fence, cancellationToken);
    }
}
