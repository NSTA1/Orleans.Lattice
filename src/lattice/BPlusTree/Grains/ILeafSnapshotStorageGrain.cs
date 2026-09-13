using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-leaf snapshot storage grain. Holds at most one
/// <see cref="LeafSnapshotBlob"/> per leaf, captured by the
/// maintenance grain when the owning leaf's persisted
/// <c>ProjectionCheckpointOffset</c> approaches the WAL retention
/// boundary (the snapshot-on-fall-off trigger of the leaf-snapshot
/// safety net).
/// <para>
/// Grain key format: the <see cref="System.Guid"/> portion of the
/// owning <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>'s
/// <see cref="GrainId"/>. One activation per leaf; the grain is
/// completely passive between capture and reactivation reads.
/// </para>
/// <para>
/// Decoupled from the leaf state row by design: the leaf row was
/// collapsed to a small fixed-shape envelope (topology, checkpoint,
/// and digest only), and a snapshot blob can be multi-MB on a leaf
/// with many live keys.
/// Persisting the blob to a separate grain row keeps the foreground
/// leaf state row small and lets the Orleans storage provider's
/// per-row limit apply only to the snapshot, not to the hot
/// foreground leaf state.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafSnapshotStorageGrain)]
internal interface ILeafSnapshotStorageGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Persists <paramref name="blob"/> as the current snapshot for
    /// this leaf, overwriting any previously persisted blob. The
    /// call returns only after the underlying state provider has
    /// durably accepted the write.
    /// </summary>
    /// <param name="blob">Snapshot payload. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the persist call.</param>
    Task SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the most recently persisted snapshot for this leaf,
    /// or <see langword="null"/> when no snapshot has ever been
    /// captured (or after a successful <see cref="ClearAsync"/>).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the encoded frame of segment <paramref name="index"/> of a
    /// segmented snapshot, or <see langword="null"/> when this leaf's snapshot
    /// is not segmented, the index is out of range, or the segment does not
    /// read back.
    /// <para>
    /// This is the read that makes the hydration allocation bounded. A
    /// segmented snapshot's payload is spread across one grain-state row per
    /// segment, so each call materialises at most one segment window rather
    /// than the whole snapshot, and the caller folds each segment's rows into
    /// its entry cache and releases the frame before requesting the next. The
    /// caller must treat a <see langword="null"/> for an in-range index as "the
    /// snapshot is not usable" and fall back to WAL replay, never as an empty
    /// segment - a segmented snapshot reads back whole or not at all.
    /// </para>
    /// </summary>
    /// <param name="index">Zero-based segment index, below <see cref="LeafSnapshotBlob.SegmentCount"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the approximate persisted snapshot footprint for this
    /// leaf in bytes - the summed key-plus-value lengths of every row in
    /// the captured <see cref="LeafSnapshotBlob"/> - or <c>0</c> when no
    /// snapshot has been captured. Used by the byte-accurate storage-usage
    /// aggregator (<see cref="ILattice.GetStorageUsageAsync"/>) to report a
    /// tree's snapshot footprint without forcing a full blob load on the
    /// caller's behalf beyond the already-activated state row.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the read.</param>
    Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Drops the persisted snapshot. Idempotent: clearing a leaf
    /// that has no snapshot is a no-op. Used by the operator-driven
    /// projection rebuild seam so a forced rebuild does not silently
    /// rehydrate from a stale snapshot.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the clear.</param>
    Task ClearAsync(CancellationToken cancellationToken);
}
