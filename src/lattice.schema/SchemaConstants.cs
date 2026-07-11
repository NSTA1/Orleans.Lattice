namespace Orleans.Lattice.Schema;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> trees that back
/// the schema-enforcement policy store and the strict-mode dead-letter queue,
/// plus the guard that keeps that namespace from being shadowed by an application
/// tree. Like the sibling <c>Orleans.Lattice.Auth</c> package, the reserved trees
/// are ordinary user-addressable trees (they do <b>not</b> use the core
/// <c>_lattice_</c> system-tree prefix) so policy and dead-letter state are fully
/// introspectable through the standard read / scan / change-feed surface.
/// </summary>
internal static class SchemaConstants
{
    /// <summary>
    /// The shared prefix identifying every schema-owned reserved tree. A governed
    /// tree id colliding with this prefix is rejected by
    /// <see cref="ThrowIfReservedTree"/> so an application tree can never shadow
    /// the policy store or the dead-letter queue.
    /// </summary>
    internal const string ReservedTreePrefix = "sys-schema-";

    /// <summary>Tree holding per-tree enforcement policies, keyed by governed tree id.</summary>
    internal const string PolicyTree = "sys-schema-policy";

    /// <summary>
    /// Tree holding strict-mode dead-letter entries, keyed
    /// <c>{treeId}\u001f{sortableTimestamp}\u001f{key}</c> so a tree's entries are
    /// a single contiguous prefix scan in time order.
    /// </summary>
    internal const string DeadLetterTree = "sys-schema-dlq";

    /// <summary>Durable per-key history view name for <see cref="PolicyTree"/>.</summary>
    internal const string PolicyHistoryView = "sys-schema-policy-history";

    /// <summary>Field separator used inside composite dead-letter keys.</summary>
    internal const char KeySeparator = '\u001f';

    /// <summary>
    /// Ambient <c>RequestContext</c> key carrying the tree id of an in-flight
    /// merge, so the merge observer can resolve the governing policy. The core
    /// <c>LatticeMergeContext</c> (#1198) does not yet carry the tree id; until it
    /// does, the observer only activates when this key is present.
    /// </summary>
    internal const string MergeTreeIdRequestContextKey = "ols.mtree";

    /// <summary>Enumerates the reserved backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { PolicyTree, DeadLetterTree };

    /// <summary>
    /// Rejects a tree id that collides with the reserved <c>sys-schema-*</c>
    /// namespace. Used on the policy-store write path so a policy can never be
    /// scoped at (and thereby govern) the internal policy or dead-letter trees.
    /// </summary>
    /// <param name="treeId">The candidate tree id.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> starts with <see cref="ReservedTreePrefix"/>.</exception>
    internal static void ThrowIfReservedTree(string treeId, string paramName)
    {
        if (treeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tree ID '{treeId}' is reserved: names starting with '{ReservedTreePrefix}' " +
                "are reserved for the Orleans.Lattice.Schema enforcement store. Choose a tree ID " +
                $"that does not start with '{ReservedTreePrefix}'.",
                paramName);
        }
    }
}
