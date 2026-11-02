using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a shard root tells one of its leaves when the leaf decides whether it may
/// advance its projection checkpoint live (epic #5605, member 4; design note
/// section 2, "Shard-level marker", and section 4, "Overlap cannot be excluded"):
/// the shard's hand-off marker, read fresh and projected onto the leaf's key range,
/// and whether the leaf is suspected to be an orphan. Read at most once per advance
/// tick, never per write. Only the projection crosses the call, not the marker, and
/// the value is immutable, so a same-silo call copies nothing.
/// </summary>
/// <param name="Initialised">Whether the shard's marker state is initialised.</param>
/// <param name="HeldOpen">Whether a token open on the shard's marker overlaps the leaf's range.</param>
/// <param name="CloseHeads">
/// Per partition, the highest head a close on the shard's marker overlapping the leaf
/// recorded, or the marker's compacted heads; <see langword="null"/> when none bears on it.
/// </param>
/// <param name="Reachable">Whether descent for the leaf's low bound reaches the leaf itself.</param>
/// <param name="OrphanRepairInProgress">Whether the shard root is repairing orphaned leaves.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.LeafLiveAdvanceView)]
internal readonly record struct LeafLiveAdvanceView(
    [property: Id(0)] bool Initialised,
    [property: Id(1)] bool HeldOpen,
    [property: Id(2)] long[]? CloseHeads,
    [property: Id(3)] bool Reachable,
    [property: Id(4)] bool OrphanRepairInProgress)
{
    /// <summary>Projects <paramref name="marker"/> onto a leaf owning <c>[low, high)</c>.</summary>
    /// <param name="marker">The shard's hand-off marker.</param>
    /// <param name="lowInclusive">The leaf's low bound.</param>
    /// <param name="highExclusive">The leaf's high bound.</param>
    /// <param name="reachable">Whether descent reaches the leaf.</param>
    /// <param name="orphanRepairInProgress">Whether orphan repair is running.</param>
    /// <returns>The view the leaf decides on.</returns>
    public static LeafLiveAdvanceView For(
        ShardHandOffState marker,
        string? lowInclusive,
        string? highExclusive,
        bool reachable,
        bool orphanRepairInProgress)
    {
        ArgumentNullException.ThrowIfNull(marker);
        return new LeafLiveAdvanceView(
            marker.Initialised,
            HandOffMarkerCore.IsHeldOpen(marker, lowInclusive, highExclusive),
            HandOffMarkerCore.RequiredHeadsOrNull(marker, lowInclusive, highExclusive),
            reachable,
            orphanRepairInProgress);
    }
}