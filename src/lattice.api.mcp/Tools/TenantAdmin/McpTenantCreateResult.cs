namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_create</c> tool:
/// the id of the tenant that was created and the lifecycle status it was created
/// in (always <c>Active</c>).
/// </summary>
internal sealed record McpTenantCreateResult
{
    /// <summary>The tenant id that was created.</summary>
    public required string TenantId { get; init; }

    /// <summary>The lifecycle status name the tenant was created in (always <c>Active</c>).</summary>
    public required string Status { get; init; }
}
