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

    /// <summary>
    /// Activation-time rehydration seam. Consults the dedicated
    /// snapshot storage grain for a persisted blob and, when the blob
    /// is newer than the leaf's persisted
    /// <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>,
    /// repopulates the in-memory entry cache from the canonical byte
    /// rows and advances the persisted checkpoint to the snapshot's
    /// offset. The projection digest is invalidated (set to <c>null</c>)
    /// so the next read or fold lazily rebuilds it via the existing
    /// <c>EnsureProjectionHashInitialized</c> path; this preserves the
    /// canonical-full-walk hash invariant the chained internal-node
    /// fold depends on.
    /// <para>
    /// No-op preconditions: tree id unset (uninitialised leaf); no
    /// snapshot present; snapshot offset not strictly greater than the
    /// persisted checkpoint (a stale snapshot whose offset the leaf
    /// has already run past). After a successful rehydrate the caller
    /// (the activation hook) drives the WAL tail-replay from the new
    /// checkpoint forward, so a snapshot that covers a prefix of the
    /// WAL plus tail-replayed suffix produces a projection identical
    /// to a from-zero replay.
    /// </para>
    /// </summary>
    internal async Task TryRehydrateFromSnapshotAsync(CancellationToken cancellationToken)
    {
        if (state.State.TreeId is null)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        LeafSnapshotBlob? blob;
        try
        {
            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
                context.GrainId.GetGuidKey());
            blob = await snapshotGrain.LoadAsync(cancellationToken);
        }
        catch
        {
            // Snapshot load is best-effort: a transient storage failure
            // must not block the leaf coming online. The activation
            // path falls through to the existing WAL-tail replay,
            // which can still recover the projection as long as the
            // WAL has not trimmed past the checkpoint.
            return;
        }

        if (blob is null)
        {
            return;
        }

        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (blob.SnapshotOffset <= checkpoint)
        {
            // Snapshot is older than the persisted checkpoint; the
            // leaf has already applied past the snapshot via the
            // foreground path. Ignore the blob.
            return;
        }

        // Bulk-load the canonical byte rows. We bypass StoreEntry
        // (the per-mutation LWW funnel) because the snapshot rows
        // are themselves a point-in-time projection; running them
        // through LWW would be a no-op against an empty cache but
        // would also re-fold the digest incrementally on every row.
        // We instead invalidate the digest below and let the lazy
        // full-walk recompute it.
        Cache.Clear();
        foreach (var row in blob.Rows)
        {
            Cache.StoreRow(row.Key, row.Value);
        }

        // Advance the persisted checkpoint to match the snapshot.
        // The WAL tail replay below picks up at (SnapshotOffset, head].
        state.State.ProjectionCheckpointOffset = blob.SnapshotOffset;

        // Invalidate the digest so EnsureProjectionHashInitialized's
        // lazy backfill path recomputes the canonical full-walk hash
        // over the rehydrated cache. The chained internal-node fold
        // depends on this hash matching the canonical full-walk hash
        // bit-for-bit; recomputing from scratch is the only way to
        // guarantee equivalence with a from-zero replay.
        state.State.ProjectionHash = null;

        // Drop the cached XxHash128 hasher so the next contribution
        // allocates a fresh instance. Mirrors the rebuild seam in
        // BPlusLeafGrain.ProjectionAdmin.cs - keeps the rehydrated
        // activation indistinguishable from a fresh activation.
        DisposeProjectionHasher();
    }
}