namespace Orleans.Lattice;

/// <summary>
/// The multi-tree view of a tag index: queries and reconcile passes range over
/// every subject tree the index covers. The covered-tree set is discovered from
/// an over-approximating hint cached on the index tree, self-healing to a full
/// index self-scan when the hint is absent.
/// </summary>
public interface ILatticeMultiTreeTagIndex
{
    /// <summary>The logical index name (the index tree is resolved as <c>tag-{indexName}</c>).</summary>
    string IndexName { get; }

    /// <summary>
    /// Narrows the index to a single subject tree, returning the ordinary
    /// single-tree surface bound to <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The subject tree to bind to.</param>
    ILatticeTagIndex Tree(string treeId);

    /// <summary>
    /// Opens a multi-tree intersection query: yields keys carrying <b>all</b> of
    /// <paramref name="tags"/>, across every covered tree.
    /// </summary>
    /// <param name="tags">The tags to intersect.</param>
    ILatticeMultiTreeTagQuery WithAllTags(params string[] tags);

    /// <summary>
    /// Opens a multi-tree union query: yields the de-duplicated keys carrying
    /// <b>any</b> of <paramref name="tags"/>, across every covered tree.
    /// </summary>
    /// <param name="tags">The tags to union.</param>
    ILatticeMultiTreeTagQuery WithAnyTags(params string[] tags);

    /// <summary>
    /// Enumerates the distinct tags that have at least one member key in any
    /// covered tree, in ascending ordinal order.
    /// </summary>
    /// <param name="cancellationToken">Cancels the index scan.</param>
    IAsyncEnumerable<string> TagsAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the subject trees the index currently covers (over-approximating).</summary>
    /// <param name="cancellationToken">Cancels the hint read / self-scan.</param>
    Task<IReadOnlyList<string>> CoveredTreesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs an on-demand reconcile across every covered tree, aggregating the
    /// per-tree reports. See
    /// <see cref="ILatticeTagIndex.ReconcileAsync(string?, string?, System.Threading.CancellationToken)"/>.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower key bound, or <c>null</c> for unbounded below.</param>
    /// <param name="endExclusive">Exclusive upper key bound, or <c>null</c> for unbounded above.</param>
    /// <param name="cancellationToken">Cancels the reconcile.</param>
    Task<TagReconcileReport> ReconcileAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default);
}
