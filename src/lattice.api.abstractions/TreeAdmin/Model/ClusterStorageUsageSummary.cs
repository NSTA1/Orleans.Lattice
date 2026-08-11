using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A cluster-wide storage accounting summary: total persisted bytes across every
/// tree, split by surface, plus a per-tree breakdown. The <see cref="Deep"/> flag
/// records how the numbers were obtained - a cheap cached WAL-poll aggregate
/// (default) or an expensive fresh leaf-walk that re-measures every shard.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.ClusterStorageUsageSummary)]
[Immutable]
public sealed record ClusterStorageUsageSummary
{
    /// <summary>The number of trees included in the summary.</summary>
    [Id(0)] public int TreeCount { get; init; }

    /// <summary>Bytes of write-ahead-log retained across all trees.</summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>Bytes of persisted shard snapshots across all trees.</summary>
    [Id(2)] public long SnapshotBytes { get; init; }

    /// <summary>Bytes of durable leaf state persisted across all trees.</summary>
    [Id(3)] public long LeafStateBytes { get; init; }

    /// <summary>
    /// Total persisted bytes across all trees: the sum of
    /// <see cref="WalRetainedBytes"/>, <see cref="SnapshotBytes"/>, and
    /// <see cref="LeafStateBytes"/>.
    /// </summary>
    [Id(4)] public long TotalBytes { get; init; }

    /// <summary>
    /// <see langword="true"/> when at least one tree reported a partial reading, so
    /// the cluster totals are a lower bound.
    /// </summary>
    [Id(5)] public bool Partial { get; init; }

    /// <summary>
    /// <see langword="true"/> when the summary came from an expensive fresh leaf-walk
    /// that re-measured every shard; <see langword="false"/> for the cheap cached
    /// WAL-poll aggregate.
    /// </summary>
    [Id(6)] public bool Deep { get; init; }

    /// <summary>The wall-clock instant the summary was sampled.</summary>
    [Id(7)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// The per-tree storage breakdowns. Never the default (empty when the cluster
    /// holds no trees).
    /// </summary>
    [Id(8)] public ImmutableArray<TreeStorageUsageSnapshot> Trees { get; init; } = ImmutableArray<TreeStorageUsageSnapshot>.Empty;
}
