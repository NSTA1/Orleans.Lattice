namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire response for the <c>GetTenantRule</c> RPC. A dedicated wrapper is required
/// because the facade answers <see langword="null"/> for a rule that does not exist,
/// and a gRPC message is never null; an absent rule travels as a lookup whose
/// <see cref="Rule"/> is <see langword="null"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminRuleLookup)]
[Immutable]
public sealed record TenantAdminRuleLookup
{
    /// <summary>The rule, or <see langword="null"/> when the tenant has no tenant-tier rule of that id.</summary>
    [Id(0)] public TenantRuleView? Rule { get; init; }
}
