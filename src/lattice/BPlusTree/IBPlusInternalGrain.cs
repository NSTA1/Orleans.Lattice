namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// An internal (non-leaf) node grain in the B+ tree. Stores separator keys
/// and references to child grains (which may be internal or leaf nodes).
/// </summary>
public interface IBPlusInternalGrain : IGrainWithGuidKey
{
    /// <summary>Initialises this internal node with the result of a root split.</summary>
    Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild, bool childrenAreLeaves);

    /// <summary>
    /// Routes a key down to the appropriate child grain and returns whether
    /// this node's children are leaves, in a single call. Avoids two
    /// sequential RPCs during tree traversal.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> RouteWithMetadataAsync(string key);

    /// <summary>Returns the grain identity of the leftmost child.</summary>
    Task<GrainId> GetLeftmostChildAsync();

    /// <summary>Returns the grain identity of the rightmost child.</summary>
    Task<GrainId> GetRightmostChildAsync();

    /// <summary>
    /// Returns the leftmost child and whether this node's children are leaves, in a single call.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetLeftmostChildWithMetadataAsync();

    /// <summary>
    /// Returns the rightmost child and whether this node's children are leaves, in a single call.
    /// </summary>
    Task<(GrainId ChildId, bool ChildrenAreLeaves)> GetRightmostChildWithMetadataAsync();

    /// <summary>Returns whether this node's children are leaf grains.</summary>
    Task<bool> AreChildrenLeavesAsync();

    /// <summary>Accepts a promoted split from a child node.</summary>
    /// <returns>A <see cref="SplitResult"/> if this node itself needed to split, otherwise <c>null</c>.</returns>
    Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild);

    /// <summary>
    /// Associates this node with a tree, enabling named options resolution.
    /// Called once by the shard root after creating the grain. Idempotent.
    /// </summary>
    Task SetTreeIdAsync(string treeId);

    /// <summary>
    /// Initialises this internal node with a pre-built list of children.
    /// Used by bulk load to construct internal nodes in a single call.
    /// <paramref name="separatorKeys"/> and <paramref name="childIds"/> must have equal length.
    /// The first separator key must be <c>null</c> (leftmost catch-all).
    /// </summary>
    Task InitializeWithChildrenAsync(List<string?> separatorKeys, List<GrainId> childIds, bool childrenAreLeaves);

    /// <summary>
    /// Returns the grain identities of all children of this internal node.
    /// Used during tree purge to enumerate the tree structure.
    /// </summary>
    Task<List<GrainId>> GetChildIdsAsync();

    /// <summary>
    /// Clears all persistent state for this grain and deactivates it.
    /// Used during tree purge to permanently remove internal node data.
    /// </summary>
    Task ClearGrainStateAsync();
}
