namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only, point-in-time summary of a single tree's state, aggregated by
/// the cluster state API from the core diagnostics surface and tree registry.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeStateSummary)]
[Immutable]
public sealed record TreeStateSummary
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Lifecycle state of the tree.</summary>
    [Id(1)] public TreeLifecycleState Lifecycle { get; init; }

    /// <summary>Number of physical shards currently owning virtual slots.</summary>
    [Id(2)] public int ShardCount { get; init; }

    /// <summary>Total live (non-tombstoned) keys across all shards.</summary>
    [Id(3)] public long TotalLiveKeys { get; init; }

    /// <summary>
    /// Total tombstoned / expired entries across all shards. Populated only
    /// when the summary was computed with a deep (tombstone-counting) read;
    /// otherwise <c>0</c>.
    /// </summary>
    [Id(4)] public long TombstoneCount { get; init; }

    /// <summary>Minimum B+ tree depth across the tree's shards.</summary>
    [Id(5)] public int MinDepth { get; init; }

    /// <summary>Maximum B+ tree depth across the tree's shards.</summary>
    [Id(6)] public int MaxDepth { get; init; }

    /// <summary>Number of shards reporting a split in progress.</summary>
    [Id(7)] public int ShardsSplitting { get; init; }

    /// <summary>Effective per-tree configuration.</summary>
    [Id(8)] public TreeConfigSummary? Config { get; init; }

    /// <summary>UTC time at which the underlying sample was assembled.</summary>
    [Id(9)] public DateTimeOffset SampledAt { get; init; }
}
