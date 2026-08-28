using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Region;

/// <summary>
/// One tenant's standing in one region, attached to a
/// <see cref="LatticeRegionDescriptor"/> when the catalog answered a
/// tenant-scoped discovery call. Tells a tenant caller not merely <em>that</em> a
/// region is targetable but <em>what its relationship with it is</em>: whether an
/// operator has authorized the region for the tenant, and where the tenant's data
/// stands in it.
/// </summary>
/// <remarks>
/// <para>
/// Present only on a tenant-scoped answer. A catalog answering an operator call,
/// a call with no asserted tenant, or a call in a cluster with no tenancy add-on
/// leaves <see cref="LatticeRegionDescriptor.TenantScope"/> <see langword="null"/>,
/// so the advertised topology is byte-for-byte what it was before tenant scoping
/// existed.
/// </para>
/// <para>
/// The two fields are independent. A region can be <see cref="IsAllowed"/> with
/// <see cref="Status"/> <see cref="TenantRegionLifecycleStatus.None"/> - authorized
/// but not yet moved into, the state a residency call transitions out of - and a
/// region can carry a live status while an operator revocation is in flight.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiRegionTypeAliases.LatticeRegionTenantScope)]
[Immutable]
public sealed record LatticeRegionTenantScope
{
    /// <summary>
    /// The tenant this standing describes. The tenant the call asserted, echoed so
    /// a multi-tenant client can attribute a cached catalog to the right tenant.
    /// </summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// <see langword="true"/> when an operator has authorized this region for the
    /// tenant (the region is in the tenant's allowed set), so the tenant may place
    /// residency there.
    /// </summary>
    [Id(1)] public required bool IsAllowed { get; init; }

    /// <summary>
    /// The tenant's per-region residency lifecycle status.
    /// <see cref="TenantRegionLifecycleStatus.None"/> when the tenant has no
    /// residency in the region.
    /// </summary>
    [Id(2)] public required TenantRegionLifecycleStatus Status { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tenant is resident in this region: its
    /// <see cref="Status"/> is <see cref="TenantRegionLifecycleStatus.Provisioning"/>,
    /// <see cref="TenantRegionLifecycleStatus.Backfilling"/>, or
    /// <see cref="TenantRegionLifecycleStatus.Online"/>. Only an
    /// <see cref="TenantRegionLifecycleStatus.Online"/> region serves the tenant's
    /// data-plane calls.
    /// </summary>
    [Id(3)] public required bool IsResident { get; init; }
}
