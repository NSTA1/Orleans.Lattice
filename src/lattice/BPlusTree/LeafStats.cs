
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-leaf counter snapshot returned by <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetStatsAsync"/>.
/// Used by the diagnostics aggregation path to compute tombstone ratios
/// without streaming entries across the grain boundary. Never exposed on
/// <see cref="ILattice"/>; the public diagnostics surface is
/// <see cref="TreeDiagnosticReport"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafStats)]
[Immutable]
internal readonly record struct LeafStats
{
    /// <summary>Number of live (non-tombstoned, non-expired) entries.</summary>
    [Id(0)] public int LiveKeys { get; init; }

    /// <summary>
    /// Number of tombstoned-or-expired entries still held pending the
    /// next compaction pass.
    /// </summary>
    [Id(1)] public int Tombstones { get; init; }

    /// <summary>
    /// Approximate retained state byte footprint of this leaf - the
    /// summed UTF-8 key length plus stored value byte length across every
    /// entry (live and tombstoned) the leaf currently holds. Fed into the
    /// byte-accurate storage-usage aggregator
    /// (<see cref="ILattice.GetStorageUsageAsync"/>) so a tree's leaf-state
    /// footprint is summable without streaming entries across the grain
    /// boundary. The figure counts the logical payload bytes a leaf holds;
    /// it excludes per-entry CRDT metadata (HLC, version vector, origin id)
    /// and Orleans persistence framing, which are backend-specific and not
    /// part of the logical key/value size. Wire-compatible: legacy
    /// <see cref="LeafStats"/> values without this field decode to <c>0</c>.
    /// </summary>
    [Id(2)] public long StateBytes { get; init; }
}

