namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of a tenant admin setting a tenant's residency set: the tenant id,
/// the regions this call began adding (transitioned to
/// <see cref="TenantRegionLifecycleStatus.Provisioning"/>) and removing
/// (transitioned to <see cref="TenantRegionLifecycleStatus.Draining"/>), and the
/// resulting per-region status rows (ordered by region id).
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantResidencyChangeResult)]
[Immutable]
public sealed record TenantResidencyChangeResult
{
    /// <summary>The tenant id whose residency set was changed.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The regions this call began adding (now provisioning), ordered.</summary>
    [Id(1)] public required IReadOnlyList<string> AddedRegions { get; init; }

    /// <summary>The regions this call began removing (now draining), ordered.</summary>
    [Id(2)] public required IReadOnlyList<string> RemovedRegions { get; init; }

    /// <summary>The resulting per-region status rows after the call, ordered by region id.</summary>
    [Id(3)] public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }
}
