using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A whole-tree read/write hotness report: one <see cref="ShardHotnessSnapshot"/>
/// per physical shard plus tree-level roll-ups, sampled at a single instant. A
/// cheap, non-blocking read used to spot skew (a small number of hot shards) so
/// an operator can decide whether a reshard is warranted.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeHotnessReport)]
[Immutable]
public sealed record TreeHotnessReport
{
    /// <summary>The logical tree id the report was sampled from.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The number of physical shards backing the tree at sample time.</summary>
    [Id(1)] public int ShardCount { get; init; }

    /// <summary>Sum of <see cref="ShardHotnessSnapshot.Reads"/> across all shards.</summary>
    [Id(2)] public long TotalReads { get; init; }

    /// <summary>Sum of <see cref="ShardHotnessSnapshot.Writes"/> across all shards.</summary>
    [Id(3)] public long TotalWrites { get; init; }

    /// <summary>Sum of <see cref="ShardHotnessSnapshot.OpsPerSecond"/> across all shards.</summary>
    [Id(4)] public double TotalOpsPerSecond { get; init; }

    /// <summary>The wall-clock instant the report was sampled.</summary>
    [Id(5)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// The per-shard hotness snapshots, ordered by
    /// <see cref="ShardHotnessSnapshot.ShardIndex"/>. Never the default (empty for a
    /// tree with no shards).
    /// </summary>
    [Id(6)] public ImmutableArray<ShardHotnessSnapshot> Shards { get; init; } = ImmutableArray<ShardHotnessSnapshot>.Empty;
}
