namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The read-only inspection report for one tenant returned by
/// <see cref="ILatticeTenantSelfService.GetTenantAsync"/>: the tenant's lifecycle
/// status together with its per-region residency rows. It is only ever produced
/// for a tenant the caller is authorized to access; an absent tenant and a tenant
/// the caller cannot see are unified into the same fail-closed
/// "not found" outcome at the facade, so this report never confirms the existence
/// of a tenant the caller has no right to observe.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantStatusReport)]
[Immutable]
public sealed record TenantStatusReport
{
    /// <summary>The tenant id this report describes.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The tenant's current lifecycle status.</summary>
    [Id(1)] public TenantLifecycleStatus Status { get; init; }

    /// <summary>
    /// <see langword="true"/> when this is the reserved legacy-adoption default
    /// tenant (<see cref="Orleans.Lattice.TenantId.DefaultId"/>).
    /// </summary>
    [Id(2)] public bool IsDefault { get; init; }

    /// <summary>
    /// The tenant's per-region residency rows: each operator-authorized or
    /// resident region with its current lifecycle status and whether it is in the
    /// tenant's allowed set. Empty when the tenant has no per-region residency
    /// configured.
    /// </summary>
    [Id(3)] public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }

    /// <summary>
    /// The tenant's resource quotas and burst allowance in effect. The reserved
    /// default tenant, and any tenant whose quotas have never been authored,
    /// reports <see cref="TenantQuotasDescriptor.Unbounded"/>.
    /// </summary>
    [Id(4)] public TenantQuotasDescriptor Quotas { get; init; }
}
