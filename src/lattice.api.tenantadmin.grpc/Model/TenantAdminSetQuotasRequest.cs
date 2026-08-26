namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the tenant-administration <c>SetTenantQuotas</c> control-API
/// RPC, carrying the target tenant id and the quotas to author for it. Unlike the
/// lifecycle RPCs (which share <see cref="TenantAdminTenantRequest"/>), quota
/// authoring needs the full <see cref="TenantQuotasDescriptor"/> payload, so it
/// has its own request message.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminSetQuotasRequest)]
[Immutable]
public sealed record TenantAdminSetQuotasRequest
{
    /// <summary>The tenant id whose quotas the call authors.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The quotas to apply to the tenant.</summary>
    [Id(1)] public TenantQuotasDescriptor Quotas { get; init; }
}
