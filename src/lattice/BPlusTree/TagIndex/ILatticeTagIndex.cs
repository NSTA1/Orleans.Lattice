namespace Orleans.Lattice;

/// <summary>
/// The public tag-index surface bound to a single subject tree. Associate tags
/// with keys, query keys by tag (intersection / union), write a value and its
/// tags together, and reconcile the index against the primary tree on demand.
/// <para>
/// Membership is stored as rows in a sibling ordinary Lattice tree resolved as
/// <c>tag-{indexName}</c>, each keyed <c>tag \0 treeId \0 key</c> with a flag
/// value, built entirely on the public <see cref="ILattice"/> surface.
/// </para>
/// </summary>
public interface ILatticeTagIndex
{
    /// <summary>The logical index name (the index tree is resolved as <c>tag-{indexName}</c>).</summary>
    string IndexName { get; }

    /// <summary>The subject tree this surface is bound to.</summary>
    string TreeId { get; }

    /// <summary>Returns the per-key tag surface for <paramref name="key"/>.</summary>
    /// <param name="key">The subject-tree key.</param>
    ILatticeKeyTags Key(string key);

    /// <summary>
    /// Opens an intersection query: yields keys carrying <b>all</b> of
    /// <paramref name="tags"/>.
    /// </summary>
    /// <param name="tags">The tags to intersect.</param>
    ILatticeTagQuery WithAllTags(params string[] tags);

    /// <summary>
    /// Opens a union query: yields the de-duplicated keys carrying <b>any</b> of
    /// <paramref name="tags"/>.
    /// </summary>
    /// <param name="tags">The tags to union.</param>
    ILatticeTagQuery WithAnyTags(params string[] tags);

    /// <summary>
    /// Enumerates the distinct tags that have at least one member key in this
    /// surface's subject tree, in ascending ordinal order.
    /// </summary>
    /// <param name="cancellationToken">Cancels the index scan.</param>
    IAsyncEnumerable<string> TagsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stages a combined write of <paramref name="value"/> for
    /// <paramref name="key"/> together with the addition of <paramref name="tags"/>.
    /// The write is additive (it associates the supplied tags; it does not
    /// remove previously-associated tags - use <c>Key(key).SetAsync</c> for
    /// replace semantics). Defaults to <see cref="TagConsistency.Eventual"/>.
    /// </summary>
    /// <param name="key">The subject-tree key to write.</param>
    /// <param name="value">The value to store under <paramref name="key"/>.</param>
    /// <param name="tags">The tags to associate with the key.</param>
    ILatticeValueTagWrite SetValueWithTags(string key, byte[] value, params string[] tags);

    /// <summary>
    /// Repairs the index against the primary tree over the half-open key range
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>):
    /// every in-range membership row whose key no longer exists in the primary
    /// tree is removed. Idempotent.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower key bound, or <c>null</c> for unbounded below.</param>
    /// <param name="endExclusive">Exclusive upper key bound, or <c>null</c> for unbounded above.</param>
    /// <param name="cancellationToken">Cancels the reconcile.</param>
    Task<TagReconcileReport> ReconcileAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default);

    /// <summary>Returns the multi-tree view of this index (spanning every covered tree).</summary>
    ILatticeMultiTreeTagIndex MultiTree();
}
