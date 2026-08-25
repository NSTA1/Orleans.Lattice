using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter layer between the tenant-admin MCP tools and the
/// <see cref="ILatticeTenantAdmin"/> facade: one method per tool that maps the
/// tool's arguments onto a facade call and projects the facade result onto the
/// compact MCP DTO. These methods hold no transport, authorization, or
/// fault-translation concern - the fail-closed tenant-admin access gate lives in
/// the facade, the caller credential is stamped on the ambient context by the
/// tool delegate before the method runs, and any escaping fault is translated to
/// an actionable <see cref="ModelContextProtocol.McpException"/> at the shared
/// <see cref="CredentialStampingTool"/> invocation seam - so they are directly
/// unit-testable against a fake facade.
/// </summary>
internal static class TenantAdminToolInvocations
{
    /// <summary>Creates a new tenant in the active status.</summary>
    /// <param name="admin">The tenant-admin facade. Must not be <c>null</c>.</param>
    /// <param name="tenantId">The tenant id to create.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP create-result DTO.</returns>
    public static async Task<McpTenantCreateResult> CreateTenantAsync(
        ILatticeTenantAdmin admin,
        string tenantId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var result = await admin.CreateTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
        return TenantAdminToolMappings.ToMcp(result);
    }

    /// <summary>Suspends a tenant, transitioning it to the suspended status.</summary>
    /// <param name="admin">The tenant-admin facade. Must not be <c>null</c>.</param>
    /// <param name="tenantId">The tenant id to suspend.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP status-change DTO.</returns>
    public static async Task<McpTenantStatusChangeResult> SuspendTenantAsync(
        ILatticeTenantAdmin admin,
        string tenantId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var result = await admin.SuspendTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
        return TenantAdminToolMappings.ToMcp(result);
    }

    /// <summary>Resumes a tenant, transitioning it back to the active status.</summary>
    /// <param name="admin">The tenant-admin facade. Must not be <c>null</c>.</param>
    /// <param name="tenantId">The tenant id to resume.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP status-change DTO.</returns>
    public static async Task<McpTenantStatusChangeResult> ResumeTenantAsync(
        ILatticeTenantAdmin admin,
        string tenantId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var result = await admin.ResumeTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
        return TenantAdminToolMappings.ToMcp(result);
    }

    /// <summary>Deletes a tenant, cascading the delete to every tree it owns.</summary>
    /// <param name="admin">The tenant-admin facade. Must not be <c>null</c>.</param>
    /// <param name="tenantId">The tenant id to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The MCP delete-result DTO.</returns>
    public static async Task<McpTenantDeleteResult> DeleteTenantAsync(
        ILatticeTenantAdmin admin,
        string tenantId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var result = await admin.DeleteTenantAsync(tenantId, cancellationToken).ConfigureAwait(false);
        return TenantAdminToolMappings.ToMcp(result);
    }
}
