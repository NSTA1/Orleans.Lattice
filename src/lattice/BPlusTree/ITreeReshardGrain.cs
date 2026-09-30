
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Coordinator grain that drives an online reshard end-to-end: a grow
/// iteratively dispatches per-shard <see cref="Orleans.Lattice.BPlusTree.ITreeShardSplitGrain"/> operations
/// against the largest-slot-owning physical shards until the tree's
/// <see cref="ShardMap"/> contains at least the target number of distinct
/// physical shards, and a shrink iteratively starts
/// <see cref="ITreeShardConsolidationGrain"/> folds against the cheapest
/// adjacent shard pairs until it contains at most the target. All work happens
/// online - the tree continues to serve reads and writes throughout, and
/// virtual-slot routing is swapped atomically by each underlying split or fold.
/// <para>
/// Key format: <c>{treeId}</c>.
/// </para>
/// </summary>
[Alias(TypeAliases.ITreeReshardGrain)]
internal interface ITreeReshardGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates an online reshard that grows or shrinks the tree to
    /// <paramref name="newShardCount"/> distinct physical shards. Returns
    /// once the intent has been persisted; the actual migration runs
    /// asynchronously, anchored by a reminder so it survives silo restarts.
    /// <para>
    /// <paramref name="newShardCount"/> must be at least 2 and no greater than
    /// the smaller of <see cref="LatticeConstants.DefaultVirtualShardCount"/>
    /// (4096) and the number of virtual slots in the tree's
    /// <see cref="ShardMap"/>. A count equal to the current physical shard count
    /// is a no-op; a smaller count on a populated tree starts a shrink, which
    /// completes only once every fold it started - including the release of
    /// each retired shard's storage - has finished.
    /// Idempotent: if a reshard to the same target is already in progress,
    /// this call is a no-op. Throws <see cref="InvalidOperationException"/>
    /// if a reshard with a different target is in progress.
    /// </para>
    /// </summary>
    Task ReshardAsync(int newShardCount);

    /// <summary>
    /// Synchronously runs remaining migration work until either a dispatch
    /// cycle completes or the reshard finishes. Used by tests and internal
    /// callers to wait for reshard completion without polling.
    /// No-op if no reshard is in progress.
    /// </summary>
    Task RunReshardPassAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle - either no reshard
    /// has ever been initiated, or the last one has run to completion.
    /// Returns <c>false</c> while a reshard is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();

    /// <summary>
    /// Reports the reshard's durable intent: whether one is in flight, the
    /// physical shard count it grows the tree to, and the count the tree had when
    /// it started. The progress itself is read from the tree's shard map. Not
    /// interleaved, so it answers between turns, when the in-memory state is
    /// exactly what was last persisted. A pure read.
    /// </summary>
    Task<ReshardProgress> GetProgressAsync();
}
