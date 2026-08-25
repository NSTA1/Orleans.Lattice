namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request carrying only a tenant id, shared by every tenant-administration
/// control-API lifecycle RPC (<c>CreateTenant</c>, <c>SuspendTenant</c>,
/// <c>ResumeTenant</c>, <c>DeleteTenant</c>), each of which addresses a single
/// tenant with no further arguments.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminTenantRequest)]
[Immutable]
public sealed record TenantAdminTenantRequest
{
    /// <summary>The tenant id the call targets.</summary>
    [Id(0)] public required string TenantId { get; init; }
}
