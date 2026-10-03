namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The caller's standing towards delegated access administration of one tenant,
/// read from the tenant posture probe. Only <see cref="Delegated"/> opens the
/// tenant Access pages; every other value keeps the cluster-wide behaviour.
/// </summary>
internal enum TenantAccessStanding
{
    /// <summary>The posture could not be read: no facade, the reserved default tenant, or no answer.</summary>
    Unavailable = 0,

    /// <summary>The cluster has delegated tenant access administration switched off.</summary>
    Off = 1,

    /// <summary>The feature is on, but the caller is neither an admin of the tenant nor a platform operator.</summary>
    NotPermitted = 2,

    /// <summary>The feature is on, and the caller administers the tenant or is a platform operator.</summary>
    Delegated = 3,
}
