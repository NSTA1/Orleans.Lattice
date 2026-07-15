namespace Orleans.Lattice.Api.State;

/// <summary>
/// One sample tick of the metrics-observation feed. To keep the stream compact
/// on large clusters the feed is delta-encoded at tree granularity:
/// <see cref="Trees"/> carries only the trees whose aggregates changed since
/// the previous tick (every visible tree on the initial tick), and
/// <see cref="RemovedTreeIds"/> names trees that disappeared. An idle cluster
/// therefore produces near-empty ticks.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeMetricsSnapshot)]
[Immutable]
public sealed record TreeMetricsSnapshot
{
    /// <summary>UTC time at which this tick was sampled.</summary>
    [Id(0)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// <see langword="true"/> for the first tick of a subscription (and for a
    /// one-shot poll), which carries the full set of visible trees rather than
    /// a delta.
    /// </summary>
    [Id(1)] public bool IsInitial { get; init; }

    /// <summary>
    /// The trees whose aggregates changed since the previous tick (all visible
    /// trees on the initial tick). Empty on an idle delta tick.
    /// </summary>
    [Id(2)] public IReadOnlyList<TreeMetrics> Trees { get; init; } = Array.Empty<TreeMetrics>();

    /// <summary>
    /// Logical ids of trees that were present in the previous tick but have
    /// since disappeared (dropped or hard-deleted). Empty on the initial tick.
    /// </summary>
    [Id(3)] public IReadOnlyList<string> RemovedTreeIds { get; init; } = Array.Empty<string>();
}
