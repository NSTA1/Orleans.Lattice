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
/// owning <see cref="Grains.IBPlusLeafGrain"/>'s
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
    /// Drops the persisted snapshot. Idempotent: clearing a leaf
    /// that has no snapshot is a no-op. Used by the operator-driven
    /// projection rebuild seam so a forced rebuild does not silently
    /// rehydrate from a stale snapshot.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the clear.</param>
    Task ClearAsync(CancellationToken cancellationToken);
}
