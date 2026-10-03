namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request naming one of a tenant's tenant-tier rules by its tenant-local id,
/// shared by the <c>GetTenantRule</c> and <c>RemoveTenantRule</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminRuleRequest)]
[Immutable]
public sealed record TenantAdminRuleRequest
{
    /// <summary>The tenant id that owns the rule.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The rule's tenant-local id.</summary>
    [Id(1)] public required string RuleId { get; init; }
}
