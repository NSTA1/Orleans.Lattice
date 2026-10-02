namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One of the four caps that bound a tenant's delegated access administration in
/// the shared membership and policy trees. Unlike a quota ceiling, a blank cap is
/// never unbounded: the cluster's default cap applies.
/// </summary>
internal enum TenancyAccessCap
{
    /// <summary>The tenant's own groups.</summary>
    Groups,

    /// <summary>Membership edges into and out of the tenant's groups.</summary>
    MembershipEdges,

    /// <summary>Entries of the tenant's member set.</summary>
    MemberSubjects,

    /// <summary>The tenant's own access rules.</summary>
    TenantRules,
}
