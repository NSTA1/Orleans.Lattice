namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the read-only <c>GetCurrentTenant</c> self-service RPC. Carries
/// no fields: the call reports only the tenant the caller's own credential resolves
/// to, so the caller identity travels in the credential header, not the payload.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantSelfCurrentRequest)]
[Immutable]
public sealed record TenantSelfCurrentRequest;
