namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire response for the <c>RemoveTenantRule</c> RPC. A dedicated wrapper is
/// required because the facade answers a bare <see cref="bool"/>, which is not a
/// reference-typed gRPC message.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminRuleRemoval)]
[Immutable]
public sealed record TenantAdminRuleRemoval
{
    /// <summary><see langword="true"/> when a rule was removed; <see langword="false"/> when none existed.</summary>
    [Id(0)] public bool Removed { get; init; }
}
