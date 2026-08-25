namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of reading a tenant's per-region residency status: the tenant id and
/// one <see cref="TenantRegionStatusDescriptor"/> per region that is either in the
/// tenant's allowed set or carries a non-<see cref="TenantRegionLifecycleStatus.None"/>
/// status, ordered by region id. This is the queryable status the residency lifecycle
/// exposes.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionStatusReport)]
[Immutable]
public sealed record TenantRegionStatusReport
{
    /// <summary>The tenant id the report describes.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The per-region status rows, ordered by region id.</summary>
    [Id(1)] public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }
}
