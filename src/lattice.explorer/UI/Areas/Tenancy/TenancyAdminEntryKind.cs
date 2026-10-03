namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>What one entry of a tenant's admin set names.</summary>
internal enum TenancyAdminEntryKind
{
    /// <summary>A user.</summary>
    User = 0,

    /// <summary>A cluster group.</summary>
    ClusterGroup = 1,

    /// <summary>One of the tenant's own groups (<c>t/{tenant}/{name}</c>).</summary>
    TenantGroup = 2,

    /// <summary>A group in another tenant's namespace, which never counts as an admin of this one.</summary>
    OtherTenantGroup = 3,

    /// <summary>A user or a cluster group: which could not be read.</summary>
    Unknown = 4,
}
