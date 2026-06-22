namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// A node placed for rendering, with its abstract grid coordinates, its load
/// colour bucket, and (when leaves are collapsed) the count of leaves it owns.
/// </summary>
public sealed record PositionedNode
{
    /// <summary>The underlying structure node.</summary>
    public required TopologyNode Node { get; init; }

    /// <summary>The horizontal grid column (centre of the node's subtree).</summary>
    public double Column { get; init; }

    /// <summary>The vertical level in the visible forest (roots at level 0).</summary>
    public int Level { get; init; }

    /// <summary>
    /// The load bucket on a cool-to-hot scale, from <c>0</c> (coolest) to
    /// <see cref="TopologyLayout.LoadBuckets"/> - 1 (hottest), derived from the
    /// node's subtree key count normalised against the busiest visible node.
    /// </summary>
    public int LoadBucket { get; init; }

    /// <summary>
    /// When leaves are collapsed, the number of leaf children this node owns
    /// (shown as a badge); <c>0</c> when leaves are rendered individually or the
    /// node owns no leaves.
    /// </summary>
    public int LeafBadge { get; init; }
}
