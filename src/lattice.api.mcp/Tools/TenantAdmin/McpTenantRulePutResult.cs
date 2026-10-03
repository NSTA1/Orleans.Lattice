namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_rule_put</c> tool:
/// the tenant-tier rule as the facade persisted it.
/// </summary>
internal sealed record McpTenantRulePutResult
{
    /// <summary>The tenant that owns the rule.</summary>
    public required string TenantId { get; init; }

    /// <summary>The persisted rule.</summary>
    public required McpTenantRule Rule { get; init; }
}
