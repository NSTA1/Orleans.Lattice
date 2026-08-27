namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantEnumerationFilter"/>: prunes a tree-id
/// enumeration to the trees the active tenant owns. Replaces the core
/// <c>NullTenantEnumerationFilter</c> when the tenancy add-on is registered, so
/// an enumeration choke point (the cluster-state tree catalog, the in-cluster
/// all-tree-ids read) never hands one tenant the ids of another's trees.
/// </summary>
/// <remarks>
/// <para>
/// Ownership is derived structurally by <see cref="LatticeTenantTrees.GetOwner"/>
/// - a <c>t/{tenant}/{name}</c> id is owned by the tenant it names, a bare legacy
/// id is adopted by the reserved <see cref="TenantId.Default"/> tenant, and a
/// <c>_lattice_</c> / <c>sys-</c> id is platform-owned. Platform-owned ids are
/// deliberately <em>kept</em>: they are not tenant data, and the catalog's own
/// system-tree switch and the per-entry authorization visibility check already
/// govern them. Pruning them here would silently change what a platform operator
/// enumerating with an active tenant sees, which is not this seam's job.
/// </para>
/// <para>
/// This filter is defence in depth, not the primary boundary. A choke point only
/// consults it when an active tenant is stamped on the ambient context, so a
/// caller that asserts no tenant is not pruned here at all; that caller is
/// confined by the per-entry authorization check, which composes the same tenant
/// enforcer the write path uses and denies a tenant-scoped tree outright when no
/// active tenant is selected.
/// </para>
/// </remarks>
internal sealed class TenantEnumerationFilter : ITenantEnumerationFilter
{
    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds)
    {
        ArgumentNullException.ThrowIfNull(treeIds);

        // Fail closed: an uninitialised "no tenant" value owns nothing, so it may
        // observe only the platform-owned ids the catalog governs separately.
        var visible = new List<string>(treeIds.Count);
        foreach (var treeId in treeIds)
        {
            if (treeId is null)
            {
                continue;
            }

            var owner = LatticeTenantTrees.GetOwner(treeId);
            if (owner.IsPlatformOwned || (tenant.Value is not null && owner.Tenant.Equals(tenant)))
            {
                visible.Add(treeId);
            }
        }

        return visible;
    }
}
