namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of an explicit tenant-creation request. A create either registers
/// a brand-new tenant (and this result reports its id and initial
/// <see cref="Status"/>) or fails closed with a
/// <see cref="TenantAlreadyExistsException"/>; unlike an idempotent upsert, a
/// create never silently reuses an existing tenant, so a returned result always
/// describes a tenant this call brought into existence.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantCreationResult)]
[Immutable]
public sealed record TenantCreationResult
{
    /// <summary>The tenant id that was created.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The lifecycle status the tenant was created in (always <see cref="TenantLifecycleStatus.Active"/>).</summary>
    [Id(1)] public TenantLifecycleStatus Status { get; init; }
}
