using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ILatticeBackupTenantScope"/>: tenant-aware isolation for
/// the backup / restore engine. Registered by <c>AddLatticeTenancy</c> in place of
/// the backup package's <see cref="NullLatticeBackupTenantScope"/>, so once the
/// tenancy add-on is installed a capture is confined to the active tenant's
/// <c>t/{tenantId}/{name}</c> namespace and a restore is confined to that
/// namespace and the tenant's key quota.
/// </summary>
/// <remarks>
/// <para>
/// This is a narrow, backup-specific isolation add-on that composes with the
/// tenant auth gate (T7): the gate already governs whether a caller may capture or
/// restore a tree at all, so this scope only adds the stricter guarantee a
/// tenant-scoped backup requires - that a tenant can never reach across to another
/// tenant's tree through the backup engine, and that a restore honours the
/// tenant's quota per record. It reads tree ownership from the tree id via
/// <see cref="LatticeTenantTrees.GetOwner"/> (T0/T1) and the ambient active tenant
/// from <see cref="LatticeActiveTenantContext"/> (T2); neither touches storage.
/// </para>
/// <para>
/// A platform-owned tree and a request with no active tenant are deferred to the
/// auth gate (which fails closed for an unauthorized caller and admits a platform
/// operator via its break-glass), so this scope never fences a platform or
/// system-origin restore. Only the clear violation - an active tenant touching a
/// tree owned by a different tenant - is refused here.
/// </para>
/// </remarks>
internal sealed class TenantBackupScope(ITenantRegistry registry) : ILatticeBackupTenantScope
{
    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public void AuthorizeCapture(string treeId) => Evaluate(treeId, "captured");

    /// <inheritdoc />
    public void AuthorizeRestoreTarget(string treeId) => Evaluate(treeId, "restored into");

    /// <inheritdoc />
    public async ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
        string targetTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(targetTreeId);

        // No active tenant: a platform / system-origin restore the auth gate has
        // already authorized. Apply no tenant quota or cross-tenant filtering; the
        // admitting admission enforces nothing per record.
        var active = LatticeActiveTenantContext.Current;
        if (active is not { Value: not null } activeTenant)
        {
            return new TenantBackupRestoreAdmission(crossTenant: false, maxKeys: null);
        }

        // Defense in depth: the target tree was already checked by
        // AuthorizeRestoreTarget, so a mismatch here is unreachable in the normal
        // flow. If it ever occurs, every record is refused as cross-tenant.
        var owner = LatticeTenantTrees.GetOwner(targetTreeId);
        var crossTenant = !owner.IsTenantOwned || !activeTenant.Equals(owner.Tenant);

        long? maxKeys = null;
        if (!crossTenant)
        {
            var record = await registry.GetAsync(activeTenant, cancellationToken).ConfigureAwait(false);
            maxKeys = record?.Quotas.MaxKeys;
        }

        return new TenantBackupRestoreAdmission(crossTenant, maxKeys);
    }

    /// <summary>
    /// Refuses a capture / restore of a tree the active tenant does not own,
    /// deferring platform trees and the no-active-tenant case to the auth gate.
    /// </summary>
    /// <param name="treeId">The tree id being captured or restored into.</param>
    /// <param name="verb">The operation verb, for the thrown message.</param>
    /// <exception cref="LatticeBackupTenantIsolationException">
    /// The active tenant does not own <paramref name="treeId"/>.
    /// </exception>
    private static void Evaluate(string treeId, string verb)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var owner = LatticeTenantTrees.GetOwner(treeId);

        // A platform-owned system tree is not tenant data; the auth gate governs
        // it and tenant isolation does not apply.
        if (owner.IsPlatformOwned)
        {
            return;
        }

        // No active tenant on a tenant-owned tree: a platform / system-origin
        // flow. Defer to the auth gate, which fails closed for an unauthorized
        // caller and admits a platform operator via its break-glass.
        var active = LatticeActiveTenantContext.Current;
        if (active is not { Value: not null } activeTenant)
        {
            return;
        }

        // The active tenant may only touch a tree it owns; a cross-tenant capture /
        // restore is refused even if a cross-tenant grant would admit a read,
        // because a tenant-scoped backup is confined to its own namespace.
        if (activeTenant.Equals(owner.Tenant))
        {
            return;
        }

        throw new LatticeBackupTenantIsolationException(
            $"Tree '{treeId}' cannot be {verb} by tenant '{activeTenant}': it is owned by a "
            + "different tenant. A tenant-scoped backup is confined to its own namespace.");
    }
}
