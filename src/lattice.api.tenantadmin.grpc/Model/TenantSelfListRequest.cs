namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the read-only <c>ListAccessibleTenants</c> self-service RPC.
/// Carries no fields: the accessible set is scoped fail-closed to the tenant the
/// caller's own credential resolves to, so the caller identity travels in the
/// credential header, not the payload.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantSelfListRequest)]
[Immutable]
public sealed record TenantSelfListRequest;
