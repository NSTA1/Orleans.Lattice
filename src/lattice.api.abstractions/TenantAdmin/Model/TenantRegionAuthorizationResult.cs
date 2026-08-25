namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of an operator authorizing a tenant's allowed region set: the tenant
/// id and the resulting live allowed region ids (ordered). A tenant admin may only
/// set residency on a region in this set.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionAuthorizationResult)]
[Immutable]
public sealed record TenantRegionAuthorizationResult
{
    /// <summary>The tenant id whose allowed region set was authorized.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The live operator-authorized allowed region ids after the call, ordered.</summary>
    [Id(1)] public required IReadOnlyList<string> AllowedRegions { get; init; }
}
