namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content description of a single tenant returned by the
/// read-only tenant self-awareness tools (<c>lattice_tenant_current</c> and each
/// row of <c>lattice_tenant_list</c>): the tenant id, its lifecycle status name,
/// and whether it is the reserved default tenant. It carries only what the caller
/// is permitted to see, so it never exposes a tenant the caller cannot access.
/// </summary>
internal sealed record McpTenantDescriptor
{
    /// <summary>The tenant id this descriptor names.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle status name (for example <c>Active</c> or <c>Suspended</c>).</summary>
    public required string Status { get; init; }

    /// <summary>Whether this is the reserved legacy-adoption default tenant.</summary>
    public required bool IsDefault { get; init; }
}
