namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for a live metadata / metrics observation - either a one-shot poll
/// (<see cref="ILatticeStateMetricsObserver.SampleAsync"/>) or a sampled
/// stream (<see cref="ILatticeStateMetricsObserver.ObserveAsync"/>). The feed
/// reports low-cardinality, per-tree aggregates sourced from the structural
/// digest and the existing metrics surface; it never emits a notification per
/// data mutation.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeMetricsRequest)]
[Immutable]
public sealed record TreeMetricsRequest
{
    /// <summary>
    /// Optional set of logical tree ids to observe. When <see langword="null"/>
    /// or empty, every visible tree in the cluster is sampled.
    /// </summary>
    [Id(0)] public IReadOnlyList<string>? TreeIds { get; init; }

    /// <summary>
    /// Whether to include per-shard hotness (operations-per-second) in each
    /// tree's metrics. Off by default so an idle cluster yields stable,
    /// near-empty delta ticks.
    /// </summary>
    [Id(1)] public bool IncludeShardHotness { get; init; }

    /// <summary>
    /// Whether to roll up materialised-view apply lag per source tree into the
    /// tree's metrics. Off by default to avoid the view-stats sampling cost
    /// when a dashboard does not show view lag.
    /// </summary>
    [Id(2)] public bool IncludeViewLag { get; init; }

    /// <summary>
    /// Whether to include reserved internal system trees in the sample.
    /// Off by default.
    /// </summary>
    [Id(3)] public bool IncludeSystemTrees { get; init; }

    /// <summary>
    /// Optional per-subscription sample cadence override. When
    /// <see langword="null"/> the configured
    /// <c>LatticeApiStateOptions.MetricsSampleInterval</c> applies.
    /// Ignored by the one-shot poll.
    /// </summary>
    [Id(4)] public TimeSpan? SampleInterval { get; init; }
}
