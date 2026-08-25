namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_delete</c> tool:
/// the deleted tenant id and the number of the tenant's trees that were cascaded
/// (soft-deleted) as part of removing it, so an operator can confirm the blast
/// radius of the delete.
/// </summary>
internal sealed record McpTenantDeleteResult
{
    /// <summary>The tenant id that was deleted.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// The number of the tenant's trees that were cascaded (soft-deleted) as part
    /// of the delete. Zero when the tenant owned no trees.
    /// </summary>
    public required int CascadedTreeCount { get; init; }
}
