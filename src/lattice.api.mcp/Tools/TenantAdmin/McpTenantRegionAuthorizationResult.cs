namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the
/// <c>lattice_tenant_authorize_regions</c> tool: the tenant whose allowed region
/// set was authored and the complete resulting allowed set, so an operator can
/// confirm the authorization without a follow-up read.
/// </summary>
internal sealed record McpTenantRegionAuthorizationResult
{
    /// <summary>The tenant id whose allowed region set was authored.</summary>
    public required string TenantId { get; init; }

    /// <summary>The complete allowed region set now in effect, ordered by region id.</summary>
    public required IReadOnlyList<string> AllowedRegions { get; init; }
}
