namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Phase of an online reshard operation driven by
/// <see cref="ITreeReshardGrain"/>. Tracks whether the coordinator is still
/// picking sources and dispatching per-shard splits (grow) or shard
/// consolidations (shrink), or has reached the target physical shard count and
/// is finalising.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ReshardPhase)]
internal enum ReshardPhase
{
    /// <summary>No reshard is active.</summary>
    None = 0,

    /// <summary>
    /// Coordinator has persisted intent and target shard count, but no
    /// per-shard split has yet been dispatched.
    /// </summary>
    Planning = 1,

    /// <summary>
    /// Coordinator is moving the map towards the target shard count: a grow
    /// dispatches <see cref="ITreeShardSplitGrain.SplitAsync"/> calls against
    /// the largest-slot-owning physical shards, and a shrink starts online
    /// shard consolidations (<see cref="ITreeShardConsolidationGrain"/>) against
    /// the cheapest adjacent pairs, each bounded by
    /// <see cref="LatticeOptions.MaxConcurrentMigrations"/>, until the persisted
    /// <see cref="ShardMap"/> holds the target number of physical shards.
    /// </summary>
    Migrating = 2,

    /// <summary>
    /// Target shard count has been reached; coordinator has cleared its
    /// active flag and is about to deactivate.
    /// </summary>
    Complete = 3,
}
