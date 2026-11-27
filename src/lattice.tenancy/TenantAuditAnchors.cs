namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The admin audit anchors of the tenancy engine: the tenant record, whose write is
/// the commit point of every tenant lifecycle, quota, administrator-set,
/// member-set, grant and region mutation (#3638).
/// </summary>
internal static class TenantAuditAnchors
{
    /// <summary>The tenant record of <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant id text.</param>
    internal static LatticeAdminAuditAnchor Tenant(string tenant) =>
        new(TenantTreeNames.RegistryTree, tenant);
}
