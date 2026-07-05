namespace Orleans.Lattice.Backup;

/// <summary>
/// The public reserved-namespace guard for <c>Orleans.Lattice.Backup</c>. The
/// backup catalog will persist its manifests into reserved <c>sys-backup-*</c>
/// trees; an application tree that shadowed that namespace could corrupt the
/// catalog. This helper lets an application validate its own tree ids (for
/// example when creating trees) against the reserved namespace, mirroring the
/// sibling <c>LatticeAuthReservedTrees</c> guard.
/// </summary>
public static class LatticeBackupReservedTrees
{
    /// <summary>The reserved tree-name prefix owned by the backup package.</summary>
    public static string Prefix => BackupConstants.ReservedTreePrefix;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> collides with the
    /// reserved <c>sys-backup-*</c> namespace.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is reserved; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public static bool IsReserved(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(BackupConstants.ReservedTreePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Throws when <paramref name="treeId"/> collides with the reserved
    /// <c>sys-backup-*</c> namespace; otherwise returns.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    public static void ThrowIfReserved(string treeId, string? paramName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        BackupConstants.ThrowIfReservedTree(treeId, paramName ?? nameof(treeId));
    }
}
