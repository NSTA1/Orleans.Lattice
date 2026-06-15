namespace Orleans.Lattice;

/// <summary>
/// Extension entry points for resolving a tag index. The subject tree stays the
/// receiver (it supplies the tree segment of every membership row); the
/// <see cref="IGrainFactory"/> resolves the sibling index tree
/// (<c>tag-{indexName}</c>) and, for multi-tree narrowing, any subject tree by
/// id - mirroring the factory-keyed cross-tree pattern used by
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite"/> and
/// <see cref="LatticeQueueExtensions.GetLatticeQueue{T}"/>.
/// </summary>
public static class LatticeTagIndexExtensions
{
    /// <summary>
    /// Resolves the tag index named <paramref name="indexName"/> bound to
    /// <paramref name="tree"/> as its subject tree.
    /// </summary>
    /// <param name="tree">The subject tree whose keys are tagged.</param>
    /// <param name="grainFactory">The grain factory used to resolve the index tree and any other subject trees.</param>
    /// <param name="indexName">The logical index name; the index tree is resolved as <c>tag-{indexName}</c>.</param>
    /// <param name="allowedTrees">
    /// Optional closed allowlist of subject tree ids accepted for membership
    /// writes. When <c>null</c> the index is open: a subject tree must already
    /// be registered (have at least one write) before its keys can be tagged.
    /// </param>
    public static ILatticeTagIndex TagIndex(
        this ILattice tree,
        IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowedTrees = null)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.Create(tree, grainFactory, indexName, allowedTrees);
    }

    /// <summary>
    /// Resolves the multi-tree view of the tag index named
    /// <paramref name="indexName"/> directly from the grain factory, without
    /// pre-binding a subject tree.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to resolve the index tree and subject trees.</param>
    /// <param name="indexName">The logical index name; the index tree is resolved as <c>tag-{indexName}</c>.</param>
    /// <param name="allowedTrees">Optional closed allowlist; see <see cref="TagIndex"/>.</param>
    public static ILatticeMultiTreeTagIndex MultiTreeTagIndex(
        this IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowedTrees = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.CreateMultiTree(grainFactory, indexName, allowedTrees);
    }
}
