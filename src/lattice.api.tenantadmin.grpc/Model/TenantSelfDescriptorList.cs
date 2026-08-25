namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire response for the read-only <c>ListAccessibleTenants</c> self-service RPC:
/// the ordered set of tenants the caller is authorized to see. A dedicated wrapper
/// message is required because a bare <see cref="IReadOnlyList{T}"/> is not a
/// reference-typed gRPC message; the wrapper carries the projected
/// <see cref="TenantDescriptor"/> rows unchanged. An empty list means the caller
/// can see no tenant beyond its own resolved (default) context.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantSelfDescriptorList)]
[Immutable]
public sealed record TenantSelfDescriptorList
{
    /// <summary>The accessible tenants, ascending by id; empty when none.</summary>
    [Id(0)] public IReadOnlyList<TenantDescriptor> Tenants { get; init; } =
        Array.Empty<TenantDescriptor>();
}
