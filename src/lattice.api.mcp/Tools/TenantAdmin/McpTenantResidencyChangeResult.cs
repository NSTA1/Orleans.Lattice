namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_set_residency</c>
/// tool: which regions the change began adding, which it began draining, and the
/// tenant's complete per-region standing afterwards. Residency transitions are
/// asynchronous, so a newly added region reports <c>Provisioning</c> rather than
/// <c>Online</c> here.
/// </summary>
internal sealed record McpTenantResidencyChangeResult
{
    /// <summary>The tenant id whose residency was authored.</summary>
    public required string TenantId { get; init; }

    /// <summary>The regions the change began adding (now provisioning), ordered by region id.</summary>
    public required IReadOnlyList<string> AddedRegions { get; init; }

    /// <summary>The regions the change began removing (now draining), ordered by region id.</summary>
    public required IReadOnlyList<string> RemovedRegions { get; init; }

    /// <summary>The tenant's complete per-region standing after the change, ordered by region id.</summary>
    public required IReadOnlyList<McpTenantRegionStatusRow> Regions { get; init; }
}
