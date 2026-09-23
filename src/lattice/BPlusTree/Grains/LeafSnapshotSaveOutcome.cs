namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// What <see cref="ILeafSnapshotStorageGrain.SaveAsync"/> did with the snapshot
/// it was offered, from the point of view of the coverage the caller may now
/// record as durable (issue #3421).
/// </summary>
/// <remarks>
/// <see cref="Kept"/> is deliberately the zero value, so any path that yields
/// the default rather than an explicit answer reads as the behaviour every
/// caller had before this outcome existed, and never as a decline that would
/// hold a leaf's WAL pin down. Every current store answers explicitly, so the
/// default is never relied on within a homogeneous cluster.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotSaveOutcome)]
internal enum LeafSnapshotSaveOutcome : byte
{
    /// <summary>
    /// The snapshot the store holds after the call covers every partition at
    /// least as far as the offered snapshot did: the offer was persisted as
    /// is, merged with a stored snapshot (element-wise maximum coverage), or
    /// already covered. The caller may record the offered coverage as durable.
    /// </summary>
    Kept = 0,

    /// <summary>
    /// The store kept its existing snapshot and did not take the offer's
    /// coverage: merging was not provably safe (a segmented stored snapshot, or
    /// a stored key the offer no longer carries), or the offer's row payload
    /// did not read back. The caller must NOT record the offered coverage,
    /// because no durable snapshot reproduces it.
    /// </summary>
    Declined = 1,
}
