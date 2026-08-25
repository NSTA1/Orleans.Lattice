namespace Orleans.Lattice.Backup;

/// <summary>
/// The tenancy seam the backup / restore engine consults to keep every capture
/// and restore inside the active tenant's <c>t/{tenantId}/{name}</c> namespace and
/// within the tenant's quota. It follows the same null-default seam pattern as the
/// data-plane tenant gate: the core backup package ships only the inert
/// <see cref="NullLatticeBackupTenantScope"/> (<see cref="IsActive"/> is
/// <c>false</c>, every method a no-op), so a host with no tenancy add-on pays no
/// cost and its capture / restore semantics are byte-for-byte unchanged. The
/// tenancy add-on registers the active implementation in its place.
/// </summary>
/// <remarks>
/// <para>
/// Tree-level enforcement (<see cref="AuthorizeCapture"/> /
/// <see cref="AuthorizeRestoreTarget"/>) is invoked from the single backup
/// authorization choke point, so it covers every capture and restore entry point
/// without threading. Per-record enforcement runs through the
/// <see cref="IBackupRestoreAdmission"/> returned by
/// <see cref="BeginRestoreAsync"/>, which the restore stream consults once per
/// record.
/// </para>
/// <para>
/// The tenant a capture or restore is scoped to is the ambient active tenant
/// (<see cref="LatticeActiveTenantContext.Current"/>); the seam reads it rather
/// than taking it as a parameter, so the same choke point serves every caller.
/// </para>
/// </remarks>
public interface ILatticeBackupTenantScope
{
    /// <summary>
    /// <c>true</c> when a tenancy add-on has replaced the null default with an
    /// active implementation. The backup engine gates every tenant check on this
    /// so the tenancy-off path stays a single branch with no further work.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Verifies that capturing <paramref name="treeId"/> is permitted for the
    /// active tenant: a platform-owned tree is left to the authorization gate, and
    /// a tenant-owned tree may be captured only by its owning tenant. A tenant
    /// capturing a tree it does not own is refused so a tenant-scoped backup can
    /// never read another tenant's or a platform tree's data.
    /// </summary>
    /// <param name="treeId">The tree id being captured. Must not be <c>null</c> or empty.</param>
    /// <exception cref="LatticeBackupTenantIsolationException">
    /// The active tenant may not capture <paramref name="treeId"/>.
    /// </exception>
    void AuthorizeCapture(string treeId);

    /// <summary>
    /// Verifies that restoring into <paramref name="treeId"/> is permitted for the
    /// active tenant, using the same ownership rule as
    /// <see cref="AuthorizeCapture"/>. A tenant restoring into a tree it does not
    /// own is refused up front, before any record is streamed.
    /// </summary>
    /// <param name="treeId">The target tree id being restored into. Must not be <c>null</c> or empty.</param>
    /// <exception cref="LatticeBackupTenantIsolationException">
    /// The active tenant may not restore into <paramref name="treeId"/>.
    /// </exception>
    void AuthorizeRestoreTarget(string treeId);

    /// <summary>
    /// Opens a per-record admission controller for a restore into
    /// <paramref name="targetTreeId"/>, resolving the active tenant's quota once so
    /// the returned <see cref="IBackupRestoreAdmission"/> can decide each record
    /// without further lookups. The controller refuses (dead-letters) any record
    /// addressed outside the active tenant's namespace or beyond its key quota.
    /// </summary>
    /// <param name="targetTreeId">The tree id being restored into. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the quota resolution.</param>
    /// <returns>An admission controller the restore stream consults per record.</returns>
    ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
        string targetTreeId,
        CancellationToken cancellationToken = default);
}
