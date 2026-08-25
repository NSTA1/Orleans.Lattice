namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// A compact, read-only description of a single tenant as surfaced by the
/// tenant self-awareness facade (<see cref="ILatticeTenantSelfService"/>): the
/// tenant id, its current lifecycle status, and whether it is the reserved
/// legacy-adoption default tenant. It carries only what a caller is permitted to
/// see about a tenant it can already access - never any cross-tenant detail - so
/// it can be returned for the caller's own tenant and for each tenant the caller
/// is authorized to enumerate without leaking a tenant the caller cannot see.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantDescriptor)]
[Immutable]
public sealed record TenantDescriptor
{
    /// <summary>The tenant id this descriptor names.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The tenant's current lifecycle status.</summary>
    [Id(1)] public TenantLifecycleStatus Status { get; init; }

    /// <summary>
    /// <see langword="true"/> when this is the reserved legacy-adoption default
    /// tenant (<see cref="Orleans.Lattice.TenantId.DefaultId"/>).
    /// </summary>
    [Id(2)] public bool IsDefault { get; init; }
}
