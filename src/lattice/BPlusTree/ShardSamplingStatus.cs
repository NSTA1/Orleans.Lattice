namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One shard's sampling status, read in a single grain turn: its volatile
/// hotness counters, whether it is the source or donor of an in-flight
/// migration, and whether a bulk graft is pending. The hot-shard monitor and
/// the shard-healing orchestrator need all three per shard on every pass, so
/// returning them together replaces three grain calls per shard with one.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardSamplingStatus)]
[Immutable]
internal readonly record struct ShardSamplingStatus
{
    /// <summary>Volatile read/write counters, as <see cref="IShardRootGrain.GetHotnessAsync"/> returns them.</summary>
    [Id(0)] public ShardHotness Hotness { get; init; }

    /// <summary>Whether a split or consolidation is in flight, as <see cref="IShardRootGrain.IsSplittingAsync"/> reports it.</summary>
    [Id(1)] public bool IsSplitting { get; init; }

    /// <summary>Whether a bulk graft is pending, as <see cref="IShardRootGrain.HasPendingBulkOperationAsync"/> reports it.</summary>
    [Id(2)] public bool HasPendingBulkOperation { get; init; }
}
