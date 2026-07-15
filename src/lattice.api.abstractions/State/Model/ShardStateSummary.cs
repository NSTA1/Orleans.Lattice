namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only, point-in-time summary of a single physical shard's state.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ShardStateSummary)]
[Immutable]
public sealed record ShardStateSummary
{
    /// <summary>Zero-based physical shard index.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// B+ tree depth for this shard - <c>1</c> when the root is a leaf,
    /// <c>2</c> with one internal level, and so on.
    /// </summary>
    [Id(1)] public int Depth { get; init; }

    /// <summary>Whether this shard's root node is currently a leaf.</summary>
    [Id(2)] public bool RootIsLeaf { get; init; }

    /// <summary>Number of live (non-tombstoned) keys owned by this shard.</summary>
    [Id(3)] public long LiveKeys { get; init; }

    /// <summary>
    /// Number of tombstoned / expired entries held by this shard, when the
    /// summary was computed with a deep read; otherwise <c>0</c>.
    /// </summary>
    [Id(4)] public long Tombstones { get; init; }

    /// <summary>Observed operations-per-second hotness for this shard.</summary>
    [Id(5)] public double OpsPerSecond { get; init; }

    /// <summary>Whether the shard is currently participating in an adaptive split.</summary>
    [Id(6)] public bool SplitInProgress { get; init; }
}
