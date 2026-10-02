namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_group_list</c> tool:
/// one page of the tenant's own groups in ascending name order.
/// </summary>
internal sealed record McpTenantGroupListResult
{
    /// <summary>The tenant whose groups were listed.</summary>
    public required string TenantId { get; init; }

    /// <summary>The groups on this page.</summary>
    public required IReadOnlyList<McpTenantGroup> Groups { get; init; }

    /// <summary>The cursor to pass back for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}
