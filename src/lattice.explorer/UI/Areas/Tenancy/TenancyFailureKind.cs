namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>What kind of failure a tenant-facade call ended in.</summary>
internal enum TenancyFailureKind
{
    /// <summary>The caller holds neither operator standing nor the tenant's admin authority.</summary>
    Denied,

    /// <summary>The tenant or grant does not exist, or the caller may not see it; the two are indistinguishable.</summary>
    NotFound,

    /// <summary>The request was malformed, such as an invalid tenant id.</summary>
    Invalid,

    /// <summary>The cluster refused the change on one of its invariants, such as the last admin subject.</summary>
    Refused,

    /// <summary>The cluster could not be reached, or does not serve tenant administration.</summary>
    Unavailable,
}
