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
    /// <param name="membershipMode">
    /// How membership rows converge. <see cref="LatticeMergeMode.LwwRegister"/>
    /// (the default) writes plain presence rows and is correct and lossless for
    /// single-writer-per-key, add-mostly indexes. <see cref="LatticeMergeMode.OrFlag"/>
    /// (enable-wins) and <see cref="LatticeMergeMode.RwFlag"/> (remove-wins)
    /// author every membership row as a typed flag-CRDT delta so concurrent
    /// add/remove from multiple clusters converges; both require
    /// <paramref name="replicaId"/> and require the index tree
    /// (<c>tag-{indexName}</c>) to be declared with the matching merge mode in
    /// replication configuration. Any other mode is rejected.
    /// </param>
    /// <param name="replicaId">
    /// The dot-authoring replica identity (typically this cluster's id) used by
    /// the flag membership modes. Required and must be non-empty when
    /// <paramref name="membershipMode"/> is a flag mode; ignored (and may be
    /// <c>null</c>) under <see cref="LatticeMergeMode.LwwRegister"/>.
    /// </param>
    public static ILatticeTagIndex TagIndex(
        this ILattice tree,
        IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowedTrees = null,
        LatticeMergeMode membershipMode = LatticeMergeMode.LwwRegister,
        string? replicaId = null)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.Create(tree, grainFactory, indexName, allowedTrees, membershipMode, replicaId);
    }

    /// <summary>
    /// Resolves the multi-tree view of the tag index named
    /// <paramref name="indexName"/> directly from the grain factory, without
    /// pre-binding a subject tree.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to resolve the index tree and subject trees.</param>
    /// <param name="indexName">The logical index name; the index tree is resolved as <c>tag-{indexName}</c>.</param>
    /// <param name="allowedTrees">Optional closed allowlist; see <see cref="TagIndex"/>.</param>
    /// <param name="membershipMode">Membership convergence mode; see <see cref="TagIndex"/>.</param>
    /// <param name="replicaId">Dot-authoring replica identity for the flag membership modes; see <see cref="TagIndex"/>.</param>
    public static ILatticeMultiTreeTagIndex MultiTreeTagIndex(
        this IGrainFactory grainFactory,
        string indexName,
        IReadOnlyCollection<string>? allowedTrees = null,
        LatticeMergeMode membershipMode = LatticeMergeMode.LwwRegister,
        string? replicaId = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.CreateMultiTree(grainFactory, indexName, allowedTrees, membershipMode, replicaId);
    }
}
