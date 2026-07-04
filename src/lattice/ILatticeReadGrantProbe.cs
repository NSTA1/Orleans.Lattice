namespace Orleans.Lattice;

/// <summary>
/// Optional capability - implemented by the real authorization gate - that
/// reports whether a subject holds at least one positive (allow) grant for an
/// operation on a tree, independent of any per-key filtering.
/// </summary>
/// <remarks>
/// <para>
/// The per-request <see cref="LatticeAccessDecision"/> cannot answer this on its
/// own. A tree that carries any per-key (exact or prefix) rule yields an
/// allow-with-filter decision for <em>every</em> subject - including one with no
/// matching rule at all, whose filter simply admits nothing - so the decision's
/// <see cref="LatticeAccessDecision.Allowed"/> flag over-reports whole-tree
/// visibility. Existence-hiding (a caller that can read no key must not learn the
/// tree exists) needs this stronger, structural signal so it can tell a subject
/// with a partial (prefix) grant - which must see the tree - apart from a subject
/// with no grant at all - which must not.
/// </para>
/// <para>
/// Registered by <c>Orleans.Lattice.Auth</c> alongside the real gate. A no-auth
/// cluster never registers it, so a consumer resolves it optionally and falls
/// back to the plain decision when it is absent (there is nothing to hide when no
/// gate is enforcing).
/// </para>
/// </remarks>
internal interface ILatticeReadGrantProbe
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="subject"/> can read at
    /// least one key of <paramref name="treeId"/> under
    /// <paramref name="operation"/>: it holds at least one allow rule (a
    /// whole-tree, prefix, or exact-key grant whose effective decision at its own
    /// scope resolves to allow), it is a bootstrap administrator, or the tree's
    /// default effect is allow for a non-anonymous subject. An anonymous subject
    /// is always <see langword="false"/>. The reserved authorization namespace is
    /// governed by control-plane isolation, so only a bootstrap administrator (or
    /// an explicit matched allow) is ever visible there regardless of the default
    /// effect.
    /// </summary>
    /// <param name="treeId">The target tree id.</param>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="operation">The operation whose grant is probed (typically a read).</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><see langword="true"/> when the subject can read at least one key.</returns>
    ValueTask<bool> HasAnyGrantAsync(
        string treeId,
        LatticeSubject subject,
        LatticeOperation operation,
        CancellationToken cancellationToken = default);
}
