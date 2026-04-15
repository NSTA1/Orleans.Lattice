namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// An internal (non-leaf) node grain in the B+ tree. Stores separator keys
/// and references to child grains (which may be internal or leaf nodes).
/// </summary>
public interface IBPlusInternalGrain : IGrainWithGuidKey
{
    /// <summary>Initialises this internal node with the result of a root split.</summary>
    Task InitializeAsync(string separatorKey, GrainId leftChild, GrainId rightChild);

    /// <summary>Routes a key down to the appropriate child grain.</summary>
    Task<GrainId> RouteAsync(string key);

    /// <summary>Accepts a promoted split from a child node.</summary>
    /// <returns>A <see cref="SplitResult"/> if this node itself needed to split, otherwise <c>null</c>.</returns>
    Task<SplitResult?> AcceptSplitAsync(string promotedKey, GrainId newChild);
}
