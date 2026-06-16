namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="ILatticeTagIndexFactory"/>. Captures the grain factory and
/// the injectable <see cref="ILatticeReplicationContext"/> seam so every tag
/// index it opens derives its membership convergence mode and dot-authoring
/// local replica id from server configuration rather than per-call parameters.
/// </summary>
internal sealed class DefaultLatticeTagIndexFactory(
    IGrainFactory grainFactory,
    ILatticeReplicationContext replicationContext) : ILatticeTagIndexFactory
{
    /// <inheritdoc />
    public ILatticeTagIndex Create(ILattice tree, string indexName)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.Create(tree, grainFactory, indexName, replicationContext);
    }

    /// <inheritdoc />
    public ILatticeMultiTreeTagIndex CreateMultiTree(string indexName, IReadOnlyCollection<string>? allowedTrees = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return LatticeTagIndexContext.CreateMultiTree(grainFactory, indexName, allowedTrees, replicationContext);
    }
}
