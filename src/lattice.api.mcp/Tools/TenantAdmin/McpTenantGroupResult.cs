namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_group_upsert</c>
/// tool: the tenant group as the facade wrote it.
/// </summary>
internal sealed record McpTenantGroupResult
{
    /// <summary>The tenant that owns the group.</summary>
    public required string TenantId { get; init; }

    /// <summary>The group's tenant-local name.</summary>
    public required string Name { get; init; }

    /// <summary>The group's display name, or <see langword="null"/> when none was set.</summary>
    public string? DisplayName { get; init; }
}
