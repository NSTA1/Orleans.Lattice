namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of a tenant status transition (suspend or resume). Reports the
/// tenant's status before and after the call and whether the transition actually
/// moved the tenant (<see cref="Changed"/> is <see langword="false"/> when the
/// tenant was already in the requested status, so the call was an idempotent
/// no-op).
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantStatusChangeResult)]
[Immutable]
public sealed record TenantStatusChangeResult
{
    /// <summary>The tenant id whose status was transitioned.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle status before the transition.</summary>
    [Id(1)] public TenantLifecycleStatus PreviousStatus { get; init; }

    /// <summary>The tenant's lifecycle status after the transition.</summary>
    [Id(2)] public TenantLifecycleStatus NewStatus { get; init; }

    /// <summary>
    /// <see langword="true"/> when the call moved the tenant from
    /// <see cref="PreviousStatus"/> to a different <see cref="NewStatus"/>;
    /// <see langword="false"/> when the tenant was already in the requested
    /// status and the call was an idempotent no-op.
    /// </summary>
    [Id(3)] public bool Changed { get; init; }
}
