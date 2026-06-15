namespace Orleans.Lattice;

/// <summary>
/// A lazy, streaming tag query that spans every subject tree the index covers
/// (or a single tree once narrowed by <see cref="InTree"/>). Yields
/// <see cref="TaggedKey"/> values so callers can tell which tree each match
/// belongs to.
/// </summary>
public interface ILatticeMultiTreeTagQuery : IAsyncEnumerable<TaggedKey>
{
    /// <summary>
    /// Narrows the query to the single subject tree <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The subject tree to restrict to.</param>
    ILatticeMultiTreeTagQuery InTree(string treeId);

    /// <summary>Returns the number of (tree, key) pairs the query matches.</summary>
    /// <param name="cancellationToken">Cancels the drain.</param>
    Task<int> CountAsync(CancellationToken cancellationToken = default);
}
