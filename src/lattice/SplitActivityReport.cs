namespace Orleans.Lattice;

/// <summary>
/// Cluster-wide snapshot of in-flight shard-migration activity returned by
/// <see cref="ILatticeAdmin.GetSplitActivityAsync"/>. It is the readable
/// counterpart to the write-only <c>orleans.lattice.split.in_flight</c>
/// histogram: a caller that must <em>decide</em> something from migration activity
/// (a scale-in safety gate, an operator tool, a deployment guard) can query this
/// in-process rather than scraping a metrics pipeline.
/// <para>
/// The figure is assembled from the per-tree heartbeat footprints each autonomic
/// monitor reports every sampling pass, derived from the authoritative shard
/// <c>IsSplitting</c> status. Adaptive split sources and consolidation fold donors
/// both contribute. Each footprint carries a time-to-live, so a silo
/// that crashes mid-migration has its share reclaimed on expiry rather than pinning
/// the count above zero forever. A report is therefore a point-in-time lower
/// bound that trails real activity by at most one monitor sampling interval.
/// </para>
/// <para>
/// A tree that has never reported an in-flight migration (or whose last pass
/// reported nothing in flight) contributes no footprint at all, so an entirely idle cluster reports
/// <see cref="InFlight"/> zero and <see cref="ReportingTrees"/> zero without any
/// tree having to heartbeat.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SplitActivityReport)]
[Immutable]
public readonly record struct SplitActivityReport
{
    /// <summary>
    /// Cluster-wide count of shard migrations currently in flight, summed across
    /// every tree with a live (non-expired) reported footprint.
    /// </summary>
    [Id(0)] public int InFlight { get; init; }

    /// <summary>
    /// How many trees contributed a live footprint to <see cref="InFlight"/>.
    /// Zero when no tree reports an in-flight migration.
    /// </summary>
    [Id(1)] public int ReportingTrees { get; init; }

    /// <summary>UTC time at which this snapshot was assembled.</summary>
    [Id(2)] public DateTimeOffset ObservedAt { get; init; }

    /// <summary>
    /// Whether at least one shard migration is in flight anywhere in the cluster.
    /// Convenience projection of <see cref="InFlight"/>.
    /// </summary>
    public bool AnyInFlight => InFlight > 0;
}
