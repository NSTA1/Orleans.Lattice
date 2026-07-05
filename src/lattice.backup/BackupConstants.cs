namespace Orleans.Lattice.Backup;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> trees that will
/// back the backup catalog and manifest store, plus the guard that keeps that
/// namespace from being shadowed by an application tree. Like the sibling
/// membership (<c>sys-membership-*</c>) and authorization (<c>sys-auth-*</c>)
/// packages, the backup catalog trees are ordinary user-addressable trees that
/// carry the core <c>sys-</c> system-data prefix, so they self-register, stay
/// durable and individually auditable, yet are hidden from the default
/// cluster-state tree catalog surfaced through the state API.
/// <para>
/// This scaffolding release reserves the <see cref="ReservedTreePrefix"/> so the
/// catalog / manifest release can create its trees inside a collision-free
/// namespace.
/// </para>
/// </summary>
internal static class BackupConstants
{
    /// <summary>
    /// The shared prefix identifying every backup-owned reserved tree. A
    /// governed tree id colliding with this prefix is rejected by
    /// <see cref="ThrowIfReservedTree"/> so an application tree can never shadow
    /// the backup catalog. Nested under the core <c>sys-</c> system-data prefix,
    /// so it inherits the state-catalog hiding behaviour without a core change.
    /// </summary>
    internal const string ReservedTreePrefix = "sys-backup-";

    /// <summary>
    /// Rejects a tree id that collides with the reserved <c>sys-backup-*</c>
    /// namespace, mirroring the guard the authorization and membership packages
    /// enforce on their own reserved namespaces.
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
                "are reserved for the Orleans.Lattice.Backup catalog. Choose a tree ID that " +
                $"does not start with '{ReservedTreePrefix}'.",
                paramName);
        }
    }
}
