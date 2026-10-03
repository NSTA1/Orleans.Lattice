namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_rule_get</c> tool. A
/// rule that does not exist reports <see cref="Found"/> as <see langword="false"/>
/// rather than failing the call.
/// </summary>
internal sealed record McpTenantRuleGetResult
{
    /// <summary>The tenant the read was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local rule id that was read.</summary>
    public required string RuleId { get; init; }

    /// <summary>Whether the tenant has a rule with that id.</summary>
    public required bool Found { get; init; }

    /// <summary>The rule, or <see langword="null"/> when not found.</summary>
    public McpTenantRule? Rule { get; init; }
}
