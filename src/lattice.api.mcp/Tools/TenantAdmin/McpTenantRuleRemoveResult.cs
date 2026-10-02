namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_rule_remove</c> tool.
/// </summary>
internal sealed record McpTenantRuleRemoveResult
{
    /// <summary>The tenant the removal was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local rule id.</summary>
    public required string RuleId { get; init; }

    /// <summary>Whether a rule was removed; <see langword="false"/> when none existed.</summary>
    public required bool Removed { get; init; }
}
