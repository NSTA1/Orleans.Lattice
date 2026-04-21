using System.Collections.Immutable;

namespace Orleans.Lattice;

/// <summary>
/// Tree-wide health snapshot returned by <see cref="ILattice.DiagnoseAsync"/>.
/// Aggregates per-shard structural and runtime metrics plus a bounded ring
/// buffer of recent adaptive-split events. Values are a point-in-time
/// sample; the diagnostics grain may serve repeat calls from a short
/// in-memory cache (configured via <see cref="LatticeOptions.DiagnosticsCacheTtl"/>).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeDiagnosticReport)]
[Immutable]
public readonly record struct TreeDiagnosticReport
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>Number of physical shards currently owning virtual slots.</summary>
    [Id(1)] public int ShardCount { get; init; }

    /// <summary>Total virtual slot count (fixed at tree creation, always 4096 for current trees).</summary>
    [Id(2)] public int VirtualShardCount { get; init; }

    /// <summary>Sum of <see cref="ShardDiagnosticReport.LiveKeys"/> across all shards.</summary>
    [Id(3)] public long TotalLiveKeys { get; init; }

    /// <summary>
    /// Sum of <see cref="ShardDiagnosticReport.Tombstones"/> across all shards.
    /// Always <c>0</c> when <see cref="Deep"/> is <c>false</c>.
    /// </summary>
    [Id(4)] public long TotalTombstones { get; init; }

    /// <summary>Per-shard diagnostics, ordered by shard index.</summary>
    [Id(5)] public ImmutableArray<ShardDiagnosticReport> Shards { get; init; }

    /// <summary>Most recent adaptive-split events (oldest first, bounded to 32).</summary>
    [Id(6)] public ImmutableArray<RecentSplit> RecentSplits { get; init; }

    /// <summary>UTC time at which this report was assembled.</summary>
    [Id(7)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// Whether the report includes tombstone counts. When <c>false</c>,
    /// <see cref="TotalTombstones"/> and <see cref="ShardDiagnosticReport.Tombstones"/>
    /// are <c>0</c> and <see cref="ShardDiagnosticReport.TombstoneRatio"/> is <c>0</c>.
    /// </summary>
    [Id(8)] public bool Deep { get; init; }
}
