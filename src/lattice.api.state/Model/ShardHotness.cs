namespace Orleans.Lattice.Api.State;

/// <summary>
/// Per-shard hotness sample carried in a <see cref="TreeMetrics"/> when the
/// request opts in via <see cref="TreeMetricsRequest.IncludeShardHotness"/>.
/// An aggregate (one row per physical shard), never a per-key metric.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ShardHotness)]
[Immutable]
public sealed record ShardHotness
{
    /// <summary>Zero-based physical shard index.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>Observed operations-per-second hotness for this shard.</summary>
    [Id(1)] public double OpsPerSecond { get; init; }

    /// <summary>Number of live (non-tombstoned) keys owned by this shard.</summary>
    [Id(2)] public long LiveKeys { get; init; }

    /// <summary>Whether the shard is currently participating in an adaptive split.</summary>
    [Id(3)] public bool SplitInProgress { get; init; }
}
