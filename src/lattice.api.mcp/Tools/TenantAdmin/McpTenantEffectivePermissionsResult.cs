namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_effective_permissions</c>
/// tool: the rules in effect for a subject within the tenant, optionally narrowed
/// to one tree.
/// </summary>
internal sealed record McpTenantEffectivePermissionsResult
{
    /// <summary>The tenant the report was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The subject reported on.</summary>
    public required string SubjectId { get; init; }

    /// <summary>The subject kind: <c>User</c>, <c>TenantGroup</c> or <c>ClusterGroup</c>.</summary>
    public required string SubjectKind { get; init; }

    /// <summary>The tenant-local tree the report was narrowed to, or <see langword="null"/> for every tree.</summary>
    public string? TreeName { get; init; }

    /// <summary>The rules in effect.</summary>
    public required IReadOnlyList<McpTenantRule> Rules { get; init; }
}
