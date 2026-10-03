namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>PutTenantRule</c> RPC, carrying the tenant whose rule to
/// write and the tenant-tier rule, with its tenant-local id.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminRulePutRequest)]
[Immutable]
public sealed record TenantAdminRulePutRequest
{
    /// <summary>The tenant id whose rule to write.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The tenant-tier rule to create or replace.</summary>
    [Id(1)] public required TenantRuleDraft Rule { get; init; }
}
