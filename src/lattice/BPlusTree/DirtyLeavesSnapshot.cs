using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Snapshot returned by
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync"/>:
/// the set of leaf grain ids that have observed at least one routed
/// <c>Delete</c> mutation since the last successful compaction pass
/// drained the shard-root dirty set, paired with the
/// <see cref="HybridLogicalClock"/> watermark at the moment of capture.
/// The compaction coordinator walks <see cref="DirtyLeaves"/> and then
/// passes <see cref="ObservedAdvance"/> to
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearDirtyLeavesUpToAsync"/> so that
/// deletes which arrive mid-pass are preserved for the next pass.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.DirtyLeavesSnapshot)]
[Immutable]
internal readonly record struct DirtyLeavesSnapshot
{
    /// <summary>Leaf grain ids dirty since the last successful drain.</summary>
    [Id(0)] public List<GrainId> DirtyLeaves { get; init; }

    /// <summary>
    /// HLC watermark at the moment of snapshot. Passed back to
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearDirtyLeavesUpToAsync"/> after the
    /// coordinator has compacted every leaf in <see cref="DirtyLeaves"/>.
    /// Entries marked with an HLC strictly greater than this watermark
    /// are preserved by the clear so the next pass picks them up.
    /// </summary>
    [Id(1)] public HybridLogicalClock ObservedAdvance { get; init; }
}
