namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// A tenant's cross-tenant grants, split into the two directions a tenant admin
/// needs to see: the grants the tenant has <see cref="Issued"/> (offers it made,
/// exposing its own data) and the grants <see cref="Received"/> (offers other
/// tenants made to it - the inbox it approves or rejects from). Both lists carry
/// grants in every lifecycle state, so a surface can show a pending offer, a live
/// agreement, and a closed one.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGrantReport)]
[Immutable]
public sealed record TenantGrantReport
{
    /// <summary>The tenant whose grants this report describes.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The grants this tenant issued, in ordinal grant-id order. These expose this
    /// tenant's own data to another tenant.
    /// </summary>
    [Id(1)] public required IReadOnlyList<TenantGrantDescriptor> Issued { get; init; }

    /// <summary>
    /// The grants offered to this tenant by other tenants, ordered by granting
    /// tenant id and then by grant id. A
    /// <see cref="TenantGrantLifecycleState.Pending"/> entry here is an offer this
    /// tenant's admins may approve or reject.
    /// </summary>
    [Id(2)] public required IReadOnlyList<TenantGrantDescriptor> Received { get; init; }
}
