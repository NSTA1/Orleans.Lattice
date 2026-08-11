using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A whole-tree diagnostic report: one <see cref="ShardDiagnosticSnapshot"/> per
/// physical shard plus tree-level roll-ups, sampled at a single instant. The
/// <see cref="Deep"/> flag records whether the sample walked leaf state
/// (authoritative but more expensive) or read only the cheap shard-root
/// projection.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeAdminDiagnosticReport)]
[Immutable]
public sealed record TreeAdminDiagnosticReport
{
    /// <summary>The logical tree id the report was sampled from.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards backing the tree at sample time.</summary>
    [Id(1)] public int ShardCount { get; init; }

    /// <summary>The virtual shard count (routing fan-out) configured for the tree.</summary>
    [Id(2)] public int VirtualShardCount { get; init; }

    /// <summary>Sum of <see cref="ShardDiagnosticSnapshot.LiveKeys"/> across all shards.</summary>
    [Id(3)] public long TotalLiveKeys { get; init; }

    /// <summary>Sum of <see cref="ShardDiagnosticSnapshot.Tombstones"/> across all shards.</summary>
    [Id(4)] public long TotalTombstones { get; init; }

    /// <summary>
    /// <see langword="true"/> when the sample walked leaf state (authoritative counts);
    /// <see langword="false"/> for the cheap shard-root projection.
    /// </summary>
    [Id(5)] public bool Deep { get; init; }

    /// <summary>The count of recent shard splits recorded at sample time.</summary>
    [Id(6)] public int RecentSplitCount { get; init; }

    /// <summary>The wall-clock instant the report was sampled.</summary>
    [Id(7)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// The per-shard diagnostic snapshots, ordered by
    /// <see cref="ShardDiagnosticSnapshot.ShardIndex"/>. Never the default (empty for a
    /// tree with no shards).
    /// </summary>
    [Id(8)] public ImmutableArray<ShardDiagnosticSnapshot> Shards { get; init; } = ImmutableArray<ShardDiagnosticSnapshot>.Empty;
}
