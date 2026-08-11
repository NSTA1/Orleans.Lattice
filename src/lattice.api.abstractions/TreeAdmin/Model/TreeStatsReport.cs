namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A rolled-up statistics snapshot for one tree: topology and live-key counts from
/// the cheap diagnostic projection, joined with the tree's storage byte breakdown.
/// A single-call "tree health at a glance" for a management surface.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeStatsReport)]
[Immutable]
public sealed record TreeStatsReport
{
    /// <summary>The logical tree id the statistics were sampled from.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards backing the tree at sample time.</summary>
    [Id(1)] public int ShardCount { get; init; }

    /// <summary>The virtual shard count (routing fan-out) configured for the tree.</summary>
    [Id(2)] public int VirtualShardCount { get; init; }

    /// <summary>Total count of live (non-tombstoned) keys across all shards.</summary>
    [Id(3)] public long TotalLiveKeys { get; init; }

    /// <summary>Total count of tombstoned keys across all shards.</summary>
    [Id(4)] public long TotalTombstones { get; init; }

    /// <summary>Bytes of durable leaf state persisted for the tree.</summary>
    [Id(5)] public long LeafStateBytes { get; init; }

    /// <summary>Bytes of persisted shard snapshots for the tree.</summary>
    [Id(6)] public long SnapshotBytes { get; init; }

    /// <summary>Bytes of write-ahead-log retained for the tree.</summary>
    [Id(7)] public long WalRetainedBytes { get; init; }

    /// <summary>
    /// Total persisted bytes for the tree: the sum of <see cref="LeafStateBytes"/>,
    /// <see cref="SnapshotBytes"/>, and <see cref="WalRetainedBytes"/>.
    /// </summary>
    [Id(8)] public long TotalBytes { get; init; }

    /// <summary>
    /// <see langword="true"/> when at least one storage surface could not be measured
    /// (a partial reading), so the byte totals are a lower bound.
    /// </summary>
    [Id(9)] public bool PartialStorage { get; init; }

    /// <summary>The wall-clock instant the statistics were sampled.</summary>
    [Id(10)] public DateTimeOffset SampledAt { get; init; }
}
