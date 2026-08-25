namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic per-region lifecycle status of a tenant as reported by
/// the tenant-administration control facade. It mirrors the tenancy engine's own
/// region-status enum without taking a dependency on the tenancy add-on, so the
/// shared contract package stays free of the engine's internals: the facade maps
/// between this enum and the engine status at the single implementation seam.
/// </summary>
/// <remarks>
/// A region is added through <see cref="Provisioning"/> and
/// <see cref="Backfilling"/> and becomes <see cref="Online"/> once its data has
/// backfilled; it is removed through <see cref="Draining"/> and
/// <see cref="Offline"/> and finally <see cref="Removed"/>. The resident set is the
/// regions whose status is <see cref="Provisioning"/>, <see cref="Backfilling"/>,
/// or <see cref="Online"/>; only an <see cref="Online"/> region serves clients.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionLifecycleStatus)]
public enum TenantRegionLifecycleStatus
{
    /// <summary>The region is not part of the tenant's residency (unconfigured).</summary>
    None = 0,

    /// <summary>An add has been initiated; the region is being prepared but is not yet serving.</summary>
    Provisioning = 1,

    /// <summary>The tenant's existing data is backfilling into the region; not yet serving.</summary>
    Backfilling = 2,

    /// <summary>The tenant is fully backfilled and the region serves clients and accepts steady-state writes.</summary>
    Online = 3,

    /// <summary>A remove has been initiated; the region is being drained and stops serving.</summary>
    Draining = 4,

    /// <summary>The region has drained and no longer serves the tenant.</summary>
    Offline = 5,

    /// <summary>Terminal: the region has been removed from the tenant's residency.</summary>
    Removed = 6,
}
