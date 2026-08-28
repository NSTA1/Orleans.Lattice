namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_region_status</c>
/// tool: one row per region that is either in the tenant's operator-authorized
/// allowed set or carries a non-<c>None</c> residency status, ordered by region
/// id.
/// </summary>
internal sealed record McpTenantRegionStatusResult
{
    /// <summary>The tenant id this report describes.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's per-region standing, ordered by region id.</summary>
    public required IReadOnlyList<McpTenantRegionStatusRow> Regions { get; init; }
}
