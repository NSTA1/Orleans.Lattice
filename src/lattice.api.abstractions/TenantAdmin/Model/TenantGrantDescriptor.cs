namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One cross-tenant grant as reported by the tenant-administration control
/// facade: the two tenants party to it, the scope of the granting tenant's data
/// it covers, the operations it authorizes there, and its current lifecycle
/// state. A grant authorizes nothing unless <see cref="State"/> is
/// <see cref="TenantGrantLifecycleState.Active"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGrantDescriptor)]
[Immutable]
public sealed record TenantGrantDescriptor
{
    /// <summary>The tenant that offered the grant, and whose data the grant exposes.</summary>
    [Id(0)] public required string GranterTenantId { get; init; }

    /// <summary>The tenant the grant is offered to, which must approve it before it authorizes anything.</summary>
    [Id(1)] public required string GranteeTenantId { get; init; }

    /// <summary>The scope of the granting tenant's data the grant applies to - a tree name or tree-name prefix.</summary>
    [Id(2)] public required string Scope { get; init; }

    /// <summary>The operations the grant authorizes on <see cref="Scope"/> once it is active.</summary>
    [Id(3)] public TenantGrantAccess Operations { get; init; }

    /// <summary>The grant's current lifecycle state.</summary>
    [Id(4)] public TenantGrantLifecycleState State { get; init; }

    /// <summary>
    /// The grant's opaque, stable identity within the granting tenant. Derived
    /// from the grantee and the scope, so re-offering the same grantee the same
    /// scope addresses the same grant rather than creating a second one. Useful as
    /// a list key; the mutating operations address a grant by granter, grantee,
    /// and scope rather than by this id.
    /// </summary>
    [Id(5)] public required string GrantId { get; init; }
}
