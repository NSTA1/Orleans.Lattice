namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_list</c> tool: the
/// tenants the calling credential is authorized to access, in ascending tenant-id
/// order. The list is scoped to the caller and never includes a tenant the caller
/// cannot see, so an anonymous or non-privileged caller under the default tenant
/// gets an empty list.
/// </summary>
internal sealed record McpTenantListResult
{
    /// <summary>The accessible tenants, ascending by id; empty when the caller can access none.</summary>
    public required IReadOnlyList<McpTenantDescriptor> Tenants { get; init; }
}
