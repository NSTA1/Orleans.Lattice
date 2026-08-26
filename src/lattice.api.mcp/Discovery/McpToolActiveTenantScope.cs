using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Lifts the calling MCP session's asserted active tenant onto the ambient
/// <see cref="LatticeActiveTenantContext"/> for the duration of a single tool
/// invocation, so the tenant-aware data plane the tool adapts (the per-tenant
/// write admission / quota controller and tenant-scoped tree resolution) sees the
/// caller's tenant rather than the reserved default. In-silo the stamped scope
/// flows to the grain on the Orleans request context; on a remote (split) head the
/// forwarding interceptor re-emits the ambient tenant as a gRPC metadata header,
/// so both topologies reach the same silo-side enforcement.
/// </summary>
/// <remarks>
/// The tenant is resolved fresh per invocation from the request's
/// <see cref="IHttpContextAccessor"/> through the registered
/// <see cref="ILatticeApiMcpActiveTenantBridge"/>. The cold path is
/// allocation-free: when no HTTP context is available, no bridge is registered, or
/// the request asserts no tenant, the ambient context is left untouched and a
/// shared no-op scope is returned, so a tenancy-off head is byte-for-byte
/// unchanged. The asserted tenant is re-validated against the caller's subject
/// membership downstream; this seam only carries the assertion.
/// </remarks>
internal static class McpToolActiveTenantScope
{
    /// <summary>
    /// Opens an active-tenant scope for one tool invocation. Dispose it when the
    /// facade call completes to restore the prior ambient active tenant.
    /// </summary>
    /// <param name="services">The tool invocation's request service provider.</param>
    /// <returns>A disposable that restores the prior ambient active tenant on dispose.</returns>
    public static IDisposable Stamp(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var httpContext = services.GetService<IHttpContextAccessor>()?.HttpContext;
        if (httpContext is null)
        {
            return NullScope.Instance;
        }

        var tenant = services.GetService<ILatticeApiMcpActiveTenantBridge>()?.Resolve(httpContext);

        // Cold path: no tenant asserted. Leave the ambient context untouched (and
        // allocate nothing) so a tenancy-off head is byte-for-byte unchanged.
        return tenant is null ? NullScope.Instance : LatticeActiveTenantContext.With(tenant);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
