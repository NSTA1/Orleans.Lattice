using System.Collections.Immutable;

namespace Orleans.Lattice;

/// <summary>
/// Cluster-wide byte-accurate storage roll-up returned by
/// <see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>. Reduces every
/// registered tree's <see cref="TreeStorageUsageReport"/> into summed
/// surface totals plus a per-tree breakdown. Values are a point-in-time
/// sample; per-tree figures are served from each tree's aggregator cache
/// (configured via <see cref="LatticeOptions.StorageUsageCacheTtl"/>).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ClusterStorageUsageReport)]
[Immutable]
public readonly record struct ClusterStorageUsageReport
{
    /// <summary>Number of registered trees included in the roll-up.</summary>
    [Id(0)] public int TreeCount { get; init; }

    /// <summary>Sum of <see cref="TreeStorageUsageReport.WalRetainedBytes"/> across every tree.</summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>Sum of <see cref="TreeStorageUsageReport.SnapshotBytes"/> across every tree.</summary>
    [Id(2)] public long SnapshotBytes { get; init; }

    /// <summary>Sum of <see cref="TreeStorageUsageReport.LeafStateBytes"/> across every tree.</summary>
    [Id(3)] public long LeafStateBytes { get; init; }

    /// <summary>
    /// Sum of <see cref="TreeStorageUsageReport.TotalBytes"/> across every
    /// tree. A lower bound when <see cref="Partial"/> is <c>true</c>.
    /// </summary>
    [Id(4)] public long TotalBytes { get; init; }

    /// <summary>
    /// <c>true</c> when at least one tree's report was
    /// <see cref="TreeStorageUsageReport.Partial"/>, meaning the cluster
    /// total is a lower bound rather than an exact figure. A tree whose whole
    /// report could not be fetched also contributes a partial zero rather than
    /// aborting the roll-up, so a single unreachable tree flags the cluster
    /// report instead of failing it.
    /// </summary>
    [Id(5)] public bool Partial { get; init; }

    /// <summary>Per-tree storage reports, ordered by tree id.</summary>
    [Id(6)] public ImmutableArray<TreeStorageUsageReport> Trees { get; init; }

    /// <summary>UTC time at which this roll-up was assembled.</summary>
    [Id(7)] public DateTimeOffset SampledAt { get; init; }
}
