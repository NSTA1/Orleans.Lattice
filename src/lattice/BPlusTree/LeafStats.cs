using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-leaf counter snapshot returned by <see cref="IBPlusLeafGrain.GetStatsAsync"/>.
/// Used by the diagnostics aggregation path to compute tombstone ratios
/// without streaming entries across the grain boundary. Never exposed on
/// <see cref="ILattice"/>; the public diagnostics surface is
/// <see cref="TreeDiagnosticReport"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafStats)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct LeafStats
{
    /// <summary>Number of live (non-tombstoned, non-expired) entries.</summary>
    [Id(0)] public int LiveKeys { get; init; }

    /// <summary>
    /// Number of tombstoned-or-expired entries still held pending the
    /// next compaction pass.
    /// </summary>
    [Id(1)] public int Tombstones { get; init; }
}

