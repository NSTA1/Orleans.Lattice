using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter layer between the tenant self-awareness MCP tools and the
/// read-only <see cref="ILatticeTenantSelfService"/> facade: one method per tool
/// that maps the tool's arguments onto a facade call and projects the facade
/// result onto the compact MCP DTO. These methods hold no transport,
/// authorization, or fault-translation concern - the leak-free, fail-closed
/// scoping lives in the facade, the caller credential is stamped on the ambient
/// context by the tool delegate before the method runs, and any escaping fault is
/// translated to an actionable <see cref="ModelContextProtocol.McpException"/> at
/// the shared <see cref="CredentialStampingTool"/> invocation seam - so they are
/// directly unit-testable against a fake facade.
/// </summary>
internal static class TenantSelfAwarenessToolInvocations
{
    /// <summary>Resolves the tenant the current caller is operating as.</summary>
    /// <param name="service">The tenant self-awareness facade. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP tenant-descriptor DTO for the caller's current tenant.</returns>
    public static async Task<McpTenantDescriptor> GetCurrentTenantAsync(
        ILatticeTenantSelfService service,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(service);
        var result = await service.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(false);
        return TenantSelfAwarenessToolMappings.ToMcp(result);
    }

    /// <summary>Lists the tenants the current caller is authorized to access.</summary>
    /// <param name="service">The tenant self-awareness facade. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP tenant-list DTO, scoped to the caller.</returns>
    public static async Task<McpTenantListResult> ListAccessibleTenantsAsync(
        ILatticeTenantSelfService service,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(service);
        var result = await service.ListAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
        return TenantSelfAwarenessToolMappings.ToMcp(result);
    }

    /// <summary>Reads the status and per-region residency of one accessible tenant.</summary>
    /// <param name="service">The tenant self-awareness facade. Must not be <c>null</c>.</param>
    /// <param name="tenantId">The tenant id to inspect.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP tenant-status DTO.</returns>
    public static async Task<McpTenantStatusResult> GetTenantAsync(
        ILatticeTenantSelfService service,
        string tenantId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(service);
        var result = await service.GetTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
        return TenantSelfAwarenessToolMappings.ToMcp(result);
    }
}
