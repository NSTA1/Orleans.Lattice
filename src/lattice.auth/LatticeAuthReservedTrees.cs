namespace Orleans.Lattice.Auth;

/// <summary>
/// The public reserved-namespace guard for <c>Orleans.Lattice.Auth</c>. The
/// policy store persists its rules into a reserved <c>sys-auth-*</c> tree; an
/// application tree that shadowed that namespace could corrupt the policy. This
/// helper lets an application validate its own tree ids (for example when
/// creating trees) against the reserved namespace, throwing the same error the
/// store enforces internally.
/// </summary>
public static class LatticeAuthReservedTrees
{
    /// <summary>The reserved tree-name prefix owned by the authorization package.</summary>
    public static string Prefix => AuthConstants.ReservedTreePrefix;

    /// <summary>The reserved tree id backing the authorization policy store.</summary>
    public static string PolicyTreeId => AuthConstants.PolicyTree;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> collides with the
    /// reserved <c>sys-auth-*</c> namespace.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is reserved; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsReserved(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(AuthConstants.ReservedTreePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Throws when <paramref name="treeId"/> collides with the reserved
    /// <c>sys-auth-*</c> namespace; otherwise returns.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    public static void ThrowIfReserved(string treeId, string? paramName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        AuthConstants.ThrowIfReservedTree(treeId, paramName ?? nameof(treeId));
    }
}
