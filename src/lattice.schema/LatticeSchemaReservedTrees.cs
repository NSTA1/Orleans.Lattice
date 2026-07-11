namespace Orleans.Lattice.Schema;

/// <summary>
/// The public reserved-namespace guard for <c>Orleans.Lattice.Schema</c>. The
/// enforcement store persists per-tree policies and dead-letter entries into
/// reserved <c>sys-schema-*</c> trees; an application tree that shadowed that
/// namespace could corrupt enforcement state. This helper lets an application
/// validate its own tree ids (for example when creating trees) against the
/// reserved namespace, throwing the same error the store enforces internally.
/// </summary>
public static class LatticeSchemaReservedTrees
{
    /// <summary>The reserved tree-name prefix owned by the schema package.</summary>
    public static string Prefix => SchemaConstants.ReservedTreePrefix;

    /// <summary>The reserved tree id backing the per-tree enforcement policy store.</summary>
    public static string PolicyTreeId => SchemaConstants.PolicyTree;

    /// <summary>The reserved tree id backing the strict-mode dead-letter queue.</summary>
    public static string DeadLetterTreeId => SchemaConstants.DeadLetterTree;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> collides with the
    /// reserved <c>sys-schema-*</c> namespace.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is reserved; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsReserved(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(SchemaConstants.ReservedTreePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Throws when <paramref name="treeId"/> collides with the reserved
    /// <c>sys-schema-*</c> namespace; otherwise returns.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    public static void ThrowIfReserved(string treeId, string? paramName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, paramName ?? nameof(treeId));
    }
}
