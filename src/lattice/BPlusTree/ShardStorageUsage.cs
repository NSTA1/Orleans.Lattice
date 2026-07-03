namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-shard byte-accurate storage-usage rollup returned by
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetStorageUsageAsync"/>. Carries the
/// summed leaf-state byte footprint and the summed persisted-snapshot byte
/// footprint across the shard's leaf chain, computed in a single walk so the
/// tree-level aggregator pays one RPC per shard rather than one per leaf.
/// Never exposed on <see cref="ILattice"/>; the public surface is
/// <see cref="TreeStorageUsageReport"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardStorageUsage)]
[Immutable]
internal readonly record struct ShardStorageUsage
{
    /// <summary>
    /// Summed serialized leaf-state byte footprint across every leaf in the
    /// shard's chain (each leaf's <see cref="LeafStats.StateBytes"/>).
    /// </summary>
    [Id(0)] public long LeafStateBytes { get; init; }

    /// <summary>
    /// Summed persisted-snapshot byte footprint across every leaf in the
    /// shard's chain. <c>0</c> when no leaf in the shard has a captured
    /// snapshot.
    /// </summary>
    [Id(1)] public long SnapshotBytes { get; init; }

    /// <summary>
    /// Summed live (non-tombstone) key count across every leaf in the shard's
    /// chain (each leaf's <see cref="LeafStats.LiveKeys"/>). Feeds the per-tree
    /// live-key admission aggregate. Best-effort: the shard maintains this
    /// incrementally off per-leaf footprint publishes between deep re-anchors,
    /// so a concurrent cross-leaf write may make it lag or lead the exact
    /// figure slightly, which is acceptable for admission control.
    /// </summary>
    [Id(2)] public long LiveKeys { get; init; }
}
