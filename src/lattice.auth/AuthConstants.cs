namespace Orleans.Lattice.Auth;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> tree that backs
/// the authorization policy store, plus the guard that keeps that namespace from
/// being shadowed by an application tree. Like the sibling membership package,
/// the policy tree is an ordinary user-addressable tree (it does <b>not</b> use
/// the core <c>_lattice_</c> system-tree prefix) so the policy is fully
/// introspectable through the standard read / scan / change-feed surface. The
/// <see cref="ReservedTreePrefix"/> convention reserves the namespace from
/// collision with application trees.
/// </summary>
internal static class AuthConstants
{
    /// <summary>
    /// The shared prefix identifying every authorization-owned reserved tree. A
    /// governed tree id colliding with this prefix is rejected by
    /// <see cref="ThrowIfReservedTree"/> so an application tree can never shadow
    /// the policy store.
    /// </summary>
    internal const string ReservedTreePrefix = "sys-auth-";

    /// <summary>Tree holding authorization rules, keyed <c>{treeId}\u001f{ruleId}</c> so a tree's rules are a single prefix scan.</summary>
    internal const string PolicyTree = "sys-auth-policy";

    /// <summary>Durable per-key history view name for <see cref="PolicyTree"/>.</summary>
    internal const string PolicyHistoryView = "sys-auth-policy-history";

    /// <summary>Field separator used inside composite rule keys.</summary>
    internal const char RuleKeySeparator = '\u001f';

    /// <summary>Enumerates the reserved backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { PolicyTree };

    /// <summary>
    /// Rejects a tree id that collides with the reserved <c>sys-auth-*</c>
    /// namespace. Used on the policy-store write path so a rule can never be
    /// scoped at (and thereby steer authorization over) the internal policy tree,
    /// and exposed through <see cref="LatticeAuthReservedTrees"/> so an
    /// application can validate its own tree ids at registration time.
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
                "are reserved for the Orleans.Lattice.Auth policy store. Choose a tree ID that " +
                $"does not start with '{ReservedTreePrefix}'.",
                paramName);
        }
    }
}
