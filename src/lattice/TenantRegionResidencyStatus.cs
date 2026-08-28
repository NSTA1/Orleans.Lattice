namespace Orleans.Lattice;

/// <summary>
/// The per-region lifecycle status of a tenant, as reported through the core
/// <see cref="ITenantRegionVisibilityResolver"/> seam. A tenant is added to a
/// region through <see cref="Provisioning"/> and <see cref="Backfilling"/> and
/// becomes <see cref="Online"/> once its data has backfilled; it is removed
/// through <see cref="Draining"/> and <see cref="Offline"/> and finally
/// <see cref="Removed"/>. The <b>resident</b> set is the regions whose status is
/// <see cref="Provisioning"/>, <see cref="Backfilling"/>, or <see cref="Online"/>;
/// only an <see cref="Online"/> region serves the tenant's clients.
/// </summary>
/// <remarks>
/// This is the core-owned copy of the vocabulary the tenancy add-on's own region
/// status enum carries, exactly as the shared API contracts package owns its own
/// copy: the core library cannot reference the tenancy add-on, so the seam maps
/// between the two at the single implementation point. Ordinal values are aligned
/// with both siblings so the mapping stays a trivial, auditable switch.
/// </remarks>
public enum TenantRegionResidencyStatus
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
