namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A change in a tenant's <see cref="TenantRegionStatus"/> in one region,
/// published by the <see cref="TenantResidencySnapshotMaintainer"/> when a
/// snapshot rebuild observes the local-region status of a tenant transition. It is
/// the queryable change event the residency lifecycle exposes: a registered
/// <see cref="ITenantRegionStatusChangeListener"/> receives one of these per
/// observed transition.
/// </summary>
/// <param name="Tenant">The tenant whose region status changed.</param>
/// <param name="RegionId">The region the status changed in (the local serving region).</param>
/// <param name="PreviousStatus">The status the region held before the change.</param>
/// <param name="CurrentStatus">The status the region holds after the change.</param>
public readonly record struct TenantRegionStatusChange(
    TenantId Tenant,
    string RegionId,
    TenantRegionStatus PreviousStatus,
    TenantRegionStatus CurrentStatus);
