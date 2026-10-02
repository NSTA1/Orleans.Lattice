namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire response for the <c>GetTenantGroup</c> RPC. A dedicated wrapper is required
/// because the facade answers <see langword="null"/> for a group that does not
/// exist, and a gRPC message is never null; an absent group travels as a lookup
/// whose <see cref="Group"/> is <see langword="null"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGroupLookup)]
[Immutable]
public sealed record TenantAdminGroupLookup
{
    /// <summary>The group, or <see langword="null"/> when the tenant has no group of that name.</summary>
    [Id(0)] public TenantGroupDescriptor? Group { get; init; }
}
