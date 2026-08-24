namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The lifecycle status of a tenant in the registry. A tenant is
/// <see cref="Active"/> when created and can be moved to <see cref="Suspended"/>
/// to fence its data-plane traffic without deleting its definition. The reserved
/// <see cref="TenantId.Default"/> tenant is always <see cref="Active"/>.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantStatus)]
public enum TenantStatus
{
    /// <summary>The tenant is live and its trees accept traffic.</summary>
    Active = 0,

    /// <summary>
    /// The tenant is administratively suspended: its definition is retained but
    /// its data-plane traffic is fenced.
    /// </summary>
    Suspended = 1,
}
