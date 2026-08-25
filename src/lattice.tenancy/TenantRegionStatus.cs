namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-region lifecycle status of a tenant in one region of a symmetric
/// multi-master deployment. A tenant is added to a region through
/// <see cref="Provisioning"/> and <see cref="Backfilling"/> and becomes
/// <see cref="Online"/> once its data has fully backfilled; it is removed through
/// <see cref="Draining"/> and <see cref="Offline"/> and finally
/// <see cref="Removed"/>. Only an <see cref="Online"/> region serves the tenant's
/// clients and accepts steady-state replicated writes; the resident set (the
/// regions the tenant's data is scoped to) is the regions whose status is
/// <see cref="Provisioning"/>, <see cref="Backfilling"/>, or <see cref="Online"/>.
/// </summary>
/// <remarks>
/// <see cref="Removed"/> is a terminal tombstone value rather than a deletion, so
/// a later re-add stamped with a higher clock deterministically supersedes it and
/// the per-region conflict-free merge stays monotonic.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantRegionStatus)]
public enum TenantRegionStatus
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
