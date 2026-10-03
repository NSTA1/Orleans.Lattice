namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_group_members</c>
/// tool: the direct members of one tenant group.
/// </summary>
internal sealed record McpTenantGroupMembersResult
{
    /// <summary>The tenant that owns the group.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local group name.</summary>
    public required string GroupName { get; init; }

    /// <summary>The group's direct members.</summary>
    public required IReadOnlyList<McpTenantSubject> Members { get; init; }
}
