using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a shard root tells one of its leaves when the leaf decides whether it may
/// advance its projection checkpoint live (epic #5605, member 4; design note
/// section 2, "Shard-level marker", and section 4, "Overlap cannot be excluded"):
/// the shard's hand-off marker, read fresh, and whether the leaf is suspected to be
/// an orphan. Read once per advance tick, never per write.
/// </summary>
/// <param name="Marker">The shard's hand-off marker, read from storage for this call.</param>
/// <param name="Reachable">Whether descent for the leaf's low bound reaches the leaf itself.</param>
/// <param name="OrphanRepairInProgress">Whether the shard root is repairing orphaned leaves.</param>
[GenerateSerializer]
[Alias(TypeAliases.LeafLiveAdvanceView)]
internal sealed record LeafLiveAdvanceView(
    [property: Id(0)] ShardHandOffState Marker,
    [property: Id(1)] bool Reachable,
    [property: Id(2)] bool OrphanRepairInProgress);
