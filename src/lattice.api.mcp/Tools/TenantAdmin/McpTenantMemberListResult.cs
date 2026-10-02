namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_member_list</c>
/// tool: one page of the tenant member set.
/// </summary>
internal sealed record McpTenantMemberListResult
{
    /// <summary>The tenant whose member set was listed.</summary>
    public required string TenantId { get; init; }

    /// <summary>The member-set entries on this page.</summary>
    public required IReadOnlyList<McpTenantSubject> Members { get; init; }

    /// <summary>The cursor to pass back for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}
