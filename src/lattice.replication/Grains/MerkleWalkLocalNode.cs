using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A resolved node in the local cluster's B+ tree during a Merkle walk: its
/// subtree content digest and, for internal nodes, its ordered child edges. A
/// leaf node carries an empty child list.
/// </summary>
internal readonly record struct MerkleWalkLocalNode
{
    /// <summary><see langword="true"/> when this node is a leaf.</summary>
    public bool IsLeaf { get; init; }

    /// <summary>The node's whole-subtree content digest.</summary>
    public LeafProjectionDigest Digest { get; init; }

    /// <summary>
    /// The node's ordered child edges, left to right. Empty for a leaf.
    /// </summary>
    public IReadOnlyList<MerkleWalkLocalChild> Children { get; init; }
}
