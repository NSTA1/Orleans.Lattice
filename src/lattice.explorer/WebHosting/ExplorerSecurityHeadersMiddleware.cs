using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace Orleans.Lattice.Explorer.Web;

/// <summary>
/// Emits the baseline <see cref="ExplorerSecurityHeaders"/> on every response
/// flowing through the Orleans.Lattice Explorer host branch - HTML pages, the
/// <c>_framework</c> assets, and the SignalR negotiate / hub endpoints alike -
/// so the authenticated admin console cannot be framed by a foreign origin
/// (CWE-1021). Registered by
/// <see cref="LatticeExplorerWebEndpointRouteBuilderExtensions.MapLatticeExplorer"/>,
/// so both the standalone head and any host that mounts the explorer inherit it.
/// </summary>
internal sealed class ExplorerSecurityHeadersMiddleware(RequestDelegate next)
{
    /// <summary>
    /// Sets each baseline security header only when it is not already present,
    /// so a value legitimately set elsewhere in the pipeline is preserved, then
    /// invokes the next middleware. The header values come from cached
    /// <see cref="StringValues"/>, so this per-response path allocates nothing.
    /// </summary>
    /// <param name="context">The current request's <see cref="HttpContext"/>.</param>
    /// <returns>A task that completes when the rest of the pipeline has run.</returns>
    public Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var headers = context.Response.Headers;

        if (StringValues.IsNullOrEmpty(headers.ContentSecurityPolicy))
        {
            headers.ContentSecurityPolicy = ExplorerSecurityHeaders.ContentSecurityPolicy;
        }

        if (StringValues.IsNullOrEmpty(headers.XFrameOptions))
        {
            headers.XFrameOptions = ExplorerSecurityHeaders.FrameOptions;
        }

        if (StringValues.IsNullOrEmpty(headers.XContentTypeOptions))
        {
            headers.XContentTypeOptions = ExplorerSecurityHeaders.ContentTypeOptions;
        }

        if (StringValues.IsNullOrEmpty(headers["Referrer-Policy"]))
        {
            headers["Referrer-Policy"] = ExplorerSecurityHeaders.ReferrerPolicy;
        }

        if (StringValues.IsNullOrEmpty(headers["Permissions-Policy"]))
        {
            headers["Permissions-Policy"] = ExplorerSecurityHeaders.PermissionsPolicy;
        }

        return next(context);
    }
}
