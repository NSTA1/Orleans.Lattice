namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_rule_list</c> tool:
/// one page of the tenant's editable tenant-tier rules and the read-only platform
/// rules scoped to the tenant's own trees.
/// </summary>
internal sealed record McpTenantRuleListResult
{
    /// <summary>The tenant whose rules were listed.</summary>
    public required string TenantId { get; init; }

    /// <summary>The rules on this page.</summary>
    public required IReadOnlyList<McpTenantRule> Rules { get; init; }

    /// <summary>The cursor to pass back for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}
