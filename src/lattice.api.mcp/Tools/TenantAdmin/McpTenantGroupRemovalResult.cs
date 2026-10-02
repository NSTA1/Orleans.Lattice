namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_group_remove</c>
/// tool: whether the group existed and everything its removal cascaded to.
/// </summary>
internal sealed record McpTenantGroupRemovalResult
{
    /// <summary>The tenant that owned the group.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local group name.</summary>
    public required string GroupName { get; init; }

    /// <summary>Whether a group was removed; <see langword="false"/> when none existed and nothing was cascaded.</summary>
    public required bool Removed { get; init; }

    /// <summary>How many membership edges into and out of the group were removed.</summary>
    public required int EdgesRemoved { get; init; }

    /// <summary>Whether the group was removed from the tenant member set.</summary>
    public required bool RemovedFromMemberSet { get; init; }

    /// <summary>Whether the group was removed from the tenant's admin set.</summary>
    public required bool RemovedFromAdminSet { get; init; }

    /// <summary>The tenant-local ids of the tenant rules that named the group and were removed.</summary>
    public required IReadOnlyList<string> RemovedRuleIds { get; init; }
}
