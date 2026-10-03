namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request naming one of a tenant's groups by its tenant-local name, shared by
/// the <c>GetTenantGroup</c>, <c>RemoveTenantGroup</c> and
/// <c>ListTenantGroupMembers</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGroupRequest)]
[Immutable]
public sealed record TenantAdminGroupRequest
{
    /// <summary>The tenant id that owns the group.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The group's tenant-local name.</summary>
    [Id(1)] public required string GroupName { get; init; }
}
