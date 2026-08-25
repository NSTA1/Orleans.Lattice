namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic lifecycle status of a tenant as reported by the
/// tenant-administration control facade. It mirrors the tenancy engine's own
/// status enum without taking a dependency on the tenancy add-on, so the shared
/// contract package stays free of the engine's internals: the facade maps between
/// this enum and the engine status at the single implementation seam.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantLifecycleStatus)]
public enum TenantLifecycleStatus
{
    /// <summary>The tenant is live and its trees accept traffic.</summary>
    Active = 0,

    /// <summary>
    /// The tenant is administratively suspended: its definition is retained but
    /// its data-plane traffic is fenced.
    /// </summary>
    Suspended = 1,
}
