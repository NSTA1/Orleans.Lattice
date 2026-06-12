using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A single child edge of a local internal node during a Merkle walk: the
/// separator key that opens the child's key-range, the child grain identity,
/// and whether that child is a leaf.
/// </summary>
internal readonly record struct MerkleWalkLocalChild
{
    /// <summary>
    /// The separator key opening this child's key-range, or
    /// <see langword="null"/> for the leftmost child (inherits the parent's
    /// lower bound).
    /// </summary>
    public string? SeparatorKey { get; init; }

    /// <summary>The grain identity of the child node.</summary>
    public GrainId NodeId { get; init; }

    /// <summary><see langword="true"/> when the child is a leaf grain.</summary>
    public bool ChildIsLeaf { get; init; }
}
