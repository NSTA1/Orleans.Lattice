namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request carrying a tenant id and a complete region set, shared by the two
/// mutating region-residency RPCs (<c>AuthorizeAllowedRegions</c> and
/// <c>SetTenantResidency</c>). Both take the same shape - a tenant plus the whole
/// desired set - because both are declarative replacements rather than
/// add / remove deltas: the server diffs the submitted set against the current one.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminRegionSetRequest)]
[Immutable]
public sealed record TenantAdminRegionSetRequest
{
    /// <summary>The tenant id the call targets.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The complete desired region set. Never a delta: regions absent from it are
    /// revoked (for the allowed set) or drained (for the residency set).
    /// </summary>
    [Id(1)] public IReadOnlyList<string> Regions { get; init; } = Array.Empty<string>();
}
