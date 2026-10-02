namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>UpsertTenantGroup</c> RPC, carrying the tenant that owns
/// the group and the group to create or replace.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGroupUpsertRequest)]
[Immutable]
public sealed record TenantAdminGroupUpsertRequest
{
    /// <summary>The tenant id that owns the group.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The group to create or replace, named by its tenant-local name.</summary>
    [Id(1)] public required TenantGroupDescriptor Group { get; init; }
}
