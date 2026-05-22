using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshot-capture partial for <see cref="BPlusLeafGrain"/>. Adds the
/// <see cref="IBPlusLeafGrain.CaptureSnapshotAsync"/> seam that copies
/// the per-activation entry cache into a canonical byte-row
/// <see cref="LeafSnapshotBlob"/> and persists it through the dedicated
/// <see cref="ILeafSnapshotStorageGrain"/> keyed by this leaf's grain
/// id. The capture is read-only on the leaf side - it stamps the blob
/// with the already-persisted <c>ProjectionCheckpointOffset</c> and
/// does not mutate any leaf state. Driven by the maintenance grain on
/// the <see cref="FallOffLogDecision.SnapshotPending"/> advisory; the
/// leaf grain itself does not invoke this method from any foreground
/// or activation path.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task CaptureSnapshotAsync()
    {
        // No-op for an uninitialised leaf. TreeId is assigned during
        // SetTreeIdAsync (called by the shard root on first attach);
        // without it the snapshot grain key would be meaningless and
        // there is no cache content worth persisting anyway.
        if (state.State.TreeId is null)
        {
            return;
        }

        // The "nothing applied" sentinel (-1) means the leaf has not
        // yet absorbed any WAL entry into its projection; capturing
        // an empty cache at offset -1 would create a snapshot the
        // activation path is required to ignore (every checkpoint
        // >= -1), so the work is pure overhead. Skip.
        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (checkpoint < 0)
        {
            return;
        }

        // Single-threaded copy of the cache rows under the grain
        // turn. EnumerateRows yields the SortedDictionary's
        // key-ordered KeyValuePair sequence; the resulting list is
        // a self-contained value snapshot that survives subsequent
        // foreground mutations on this activation.
        var rows = new List<LeafSnapshotRow>(Cache.Count);
        foreach (var kv in Cache.EnumerateRows())
        {
            rows.Add(new LeafSnapshotRow(kv.Key, kv.Value));
        }

        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = checkpoint,
            Rows = rows,
            CapturedAtTicks = DateTime.UtcNow.Ticks,
        };

        var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
            context.GrainId.GetGuidKey());
        await snapshotGrain.SaveAsync(blob, CancellationToken.None);
    }
}