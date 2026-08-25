namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One row of a tenant's per-region residency report: a region id, its current
/// <see cref="TenantRegionLifecycleStatus"/>, and whether it is in the tenant's
/// operator-authorized allowed set. The residency set (the regions the tenant's
/// data is scoped to) is the rows whose <see cref="Status"/> is
/// <see cref="TenantRegionLifecycleStatus.Provisioning"/>,
/// <see cref="TenantRegionLifecycleStatus.Backfilling"/>, or
/// <see cref="TenantRegionLifecycleStatus.Online"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionStatusDescriptor)]
[Immutable]
public sealed record TenantRegionStatusDescriptor
{
    /// <summary>The region id this row describes.</summary>
    [Id(0)] public required string RegionId { get; init; }

    /// <summary>The region's current lifecycle status for the tenant.</summary>
    [Id(1)] public TenantRegionLifecycleStatus Status { get; init; }

    /// <summary>
    /// <see langword="true"/> when the region is in the tenant's operator-authorized
    /// allowed set (residency may only be set on an allowed region).
    /// </summary>
    [Id(2)] public bool IsAllowed { get; init; }
}
