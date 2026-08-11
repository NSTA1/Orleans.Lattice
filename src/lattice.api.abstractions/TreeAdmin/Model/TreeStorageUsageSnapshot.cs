namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// One tree's storage byte breakdown, carried as a row of a
/// <see cref="ClusterStorageUsageSummary"/>. Splits durable footprint across the
/// three persisted surfaces (write-ahead log, snapshots, leaf state) so an operator
/// can see where a tree's bytes live.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeStorageUsageSnapshot)]
[Immutable]
public sealed record TreeStorageUsageSnapshot
{
    /// <summary>The logical tree id the breakdown is for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Bytes of write-ahead-log retained for the tree.</summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>Bytes of persisted shard snapshots for the tree.</summary>
    [Id(2)] public long SnapshotBytes { get; init; }

    /// <summary>Bytes of durable leaf state persisted for the tree.</summary>
    [Id(3)] public long LeafStateBytes { get; init; }

    /// <summary>
    /// Total persisted bytes for the tree: the sum of <see cref="WalRetainedBytes"/>,
    /// <see cref="SnapshotBytes"/>, and <see cref="LeafStateBytes"/>.
    /// </summary>
    [Id(4)] public long TotalBytes { get; init; }

    /// <summary>
    /// <see langword="true"/> when at least one storage surface could not be measured
    /// for the tree, so its byte totals are a lower bound.
    /// </summary>
    [Id(5)] public bool Partial { get; init; }

    /// <summary>Count of live (non-tombstoned) keys in the tree, when available.</summary>
    [Id(6)] public long LiveKeys { get; init; }

    /// <summary>The wall-clock instant the tree's usage was sampled.</summary>
    [Id(7)] public DateTimeOffset SampledAt { get; init; }
}
