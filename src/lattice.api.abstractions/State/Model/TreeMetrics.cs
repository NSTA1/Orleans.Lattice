namespace Orleans.Lattice.Api.State;

/// <summary>
/// Low-cardinality live aggregates for a single tree, as sampled by the
/// metrics-observation feed. Every field is a tree- or shard-level rollup
/// drawn from the structural digest and the existing metrics surface; cost is
/// bounded by tree / shard count, never by key count.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeMetrics)]
[Immutable]
public sealed record TreeMetrics
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Lifecycle state of the tree.</summary>
    [Id(1)] public TreeLifecycleState Lifecycle { get; init; }

    /// <summary>Number of physical shards currently owning virtual slots.</summary>
    [Id(2)] public int ShardCount { get; init; }

    /// <summary>Total live (non-tombstoned) keys across all shards.</summary>
    [Id(3)] public long LiveKeys { get; init; }

    /// <summary>Total tombstoned / expired entries across all shards.</summary>
    [Id(4)] public long Tombstones { get; init; }

    /// <summary>Minimum B+ tree depth across the tree's shards.</summary>
    [Id(5)] public int MinDepth { get; init; }

    /// <summary>Maximum B+ tree depth across the tree's shards.</summary>
    [Id(6)] public int MaxDepth { get; init; }

    /// <summary>Number of shards reporting a split in progress.</summary>
    [Id(7)] public int ShardsSplitting { get; init; }

    /// <summary>
    /// Number of materialised views projecting from this tree, or
    /// <see langword="null"/> when view lag was not requested
    /// (see <see cref="TreeMetricsRequest.IncludeViewLag"/>).
    /// </summary>
    [Id(8)] public int? ViewCount { get; init; }

    /// <summary>
    /// Total materialised-view apply lag (summed across the tree's views), or
    /// <see langword="null"/> when view lag was not requested or no view
    /// reported a lag sample.
    /// </summary>
    [Id(9)] public long? ViewLagTotal { get; init; }

    /// <summary>
    /// Per-shard hotness rows when
    /// <see cref="TreeMetricsRequest.IncludeShardHotness"/> is set; otherwise
    /// empty.
    /// </summary>
    [Id(10)] public IReadOnlyList<ShardHotness> ShardHotness { get; init; } = Array.Empty<ShardHotness>();

    /// <summary>
    /// <see langword="true"/> when the sampler deliberately skipped the fresh
    /// per-shard walk because the tree was reporting WAL saturation, so the
    /// live counts (<see cref="LiveKeys"/>, <see cref="Tombstones"/>,
    /// <see cref="MinDepth"/>/<see cref="MaxDepth"/>,
    /// <see cref="ShardsSplitting"/>) and <see cref="ShardHotness"/> are paused
    /// and reported as zero / empty rather than sampled. Registry-sourced fields
    /// (<see cref="Lifecycle"/>, <see cref="ShardCount"/>) and any requested view
    /// lag are still populated. The detail returns automatically once the tree
    /// settles; a consumer should surface this as a transient "paused - busy"
    /// state, not an error.
    /// </summary>
    [Id(11)] public bool DetailPaused { get; init; }
}
