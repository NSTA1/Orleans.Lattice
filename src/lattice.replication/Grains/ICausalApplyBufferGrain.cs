using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable per-tree causal-apply buffer (#4464). Grain key: <c>{treeId}</c>.
/// <para>
/// Holds the replication records the receiver could not apply yet because
/// their declared <see cref="WalRecord.VectorClock"/> dependencies were not
/// satisfied by the local vector clock. One activation per tree serializes
/// every park and drain, whichever silo received the entry, so a dependency
/// applied on one silo drains an entry parked through another. Parking is
/// durable before <see cref="ParkAsync"/> returns, so the applier may
/// acknowledge the entry; a drain removes an entry durably only after its
/// apply returned, so a crash between the two re-applies it on the next drain,
/// which is idempotent at the leaf.
/// </para>
/// <para>
/// A drain runs after every park (closing the lost wakeup between the
/// applier's dependency check and the insert), whenever
/// <see cref="DrainAsync"/> is called - after a high-water-mark advance, on a
/// silo's first touch of the tree, after the bootstrap pin - and on every
/// replication maintenance tick, which bounds how long an entry whose
/// dependencies are met can wait after a restart or under quiescence.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ICausalApplyBufferGrain)]
internal interface ICausalApplyBufferGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably parks <paramref name="entry"/>, then drains every parked entry
    /// whose dependencies are already satisfied (including, possibly,
    /// <paramref name="entry"/> itself). When the buffer is full the oldest
    /// entries are evicted to the per-tree dead-letter queue first, exactly as
    /// the bounded in-memory buffer did. Parking an entry that is already
    /// parked is a no-op. Returns the number of entries still parked.
    /// </summary>
    /// <param name="entry">The record to park.</param>
    /// <param name="admissionEpoch">
    /// The tree's receive-fence epoch, read uncached while the fence was not
    /// paused (issue #4593). The drain stamps the entry's apply with it, so a
    /// restored copy refuses - and the drain then discards - an entry parked
    /// before its restore paused receiving. A re-park of an already parked entry
    /// keeps the epoch it was first parked under.
    /// </param>
    Task<int> ParkAsync(WalRecord entry, long admissionEpoch = 0);

    /// <summary>
    /// Applies, in FIFO order and to a fixed point, every parked entry whose
    /// dependencies the local vector clock now satisfies, removing each one
    /// durably after its apply returned. Returns the number of entries still
    /// parked. Cheap when the buffer is empty.
    /// </summary>
    Task<int> DrainAsync();

    /// <summary>Returns the number of entries currently parked.</summary>
    Task<int> CountAsync();
}
