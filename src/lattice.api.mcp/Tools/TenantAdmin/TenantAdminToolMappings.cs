using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Pure projections from the tenant-admin control facade's domain results onto
/// the compact MCP structured-content DTOs the tenant-admin tools return. Kept
/// side-effect free and allocation-lean so a tool invocation maps a facade result
/// without any I/O of its own.
/// </summary>
internal static class TenantAdminToolMappings
{
    /// <summary>Projects a tenant-creation result onto its MCP DTO.</summary>
    /// <param name="result">The creation result. Must not be <c>null</c>.</param>
    /// <returns>The MCP create-result DTO.</returns>
    public static McpTenantCreateResult ToMcp(TenantCreationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpTenantCreateResult
        {
            TenantId = result.TenantId,
            Status = result.Status.ToString(),
        };
    }

    /// <summary>Projects a tenant status-change result onto its MCP DTO.</summary>
    /// <param name="result">The status-change result. Must not be <c>null</c>.</param>
    /// <returns>The MCP status-change DTO.</returns>
    public static McpTenantStatusChangeResult ToMcp(TenantStatusChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpTenantStatusChangeResult
        {
            TenantId = result.TenantId,
            PreviousStatus = result.PreviousStatus.ToString(),
            NewStatus = result.NewStatus.ToString(),
            Changed = result.Changed,
        };
    }

    /// <summary>Projects a tenant-deletion result onto its MCP DTO.</summary>
    /// <param name="result">The deletion result. Must not be <c>null</c>.</param>
    /// <returns>The MCP delete-result DTO.</returns>
    public static McpTenantDeleteResult ToMcp(TenantDeletionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpTenantDeleteResult
        {
            TenantId = result.TenantId,
            CascadedTreeCount = result.CascadedTreeCount,
        };
    }
}
