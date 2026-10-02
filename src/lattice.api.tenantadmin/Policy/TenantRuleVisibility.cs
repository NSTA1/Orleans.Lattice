namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// How a stored authorization rule appears to the administrators of one tenant,
/// decided by <see cref="TenantPolicyScope.Classify"/>.
/// </summary>
internal enum TenantRuleVisibility
{
    /// <summary>The rule does not concern the tenant: it is never shown or counted.</summary>
    Hidden = 0,

    /// <summary>One of the tenant's own tenant-tier rules: listed in full and editable.</summary>
    Tenant = 1,

    /// <summary>An operator rule scoped to one of the tenant's trees: listed in full, read-only.</summary>
    PlatformTree = 2,

    /// <summary>A cluster-wide <c>Tree:*</c> operator rule: never listed, reported by id and effect only.</summary>
    PlatformWide = 3,

    /// <summary>An app role rule on one of the tenant's app trees: never listed, reported by id and effect only.</summary>
    AppRole = 4,
}
