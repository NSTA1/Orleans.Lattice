using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A read-only view over the local cluster's B+ tree for one shard, used by the
/// Merkle-walk drift-localisation engine to fetch the shard root and resolve a
/// node's subtree digest and child edges. Implementations must never mutate
/// data or any replication cursor.
/// </summary>
internal interface IMerkleWalkLocalTree
{
    /// <summary>
    /// Resolves the shard's current root node, or <see langword="null"/> when
    /// the shard is empty (no root yet).
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    ValueTask<MerkleWalkLocalNode?> GetRootAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Resolves a node by grain identity, fetching its subtree digest and (for
    /// an internal node) its ordered child edges.
    /// </summary>
    /// <param name="nodeId">The grain identity of the node to resolve.</param>
    /// <param name="isLeaf"><see langword="true"/> when the node is a leaf.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    ValueTask<MerkleWalkLocalNode> ResolveAsync(GrainId nodeId, bool isLeaf, CancellationToken cancellationToken);
}
