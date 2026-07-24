using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Orleans.Lattice.Explorer.Core.Authentication;

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
internal sealed class ExplorerSecurityHeadersMiddleware
{
    private readonly RequestDelegate _next;

    /// <summary>
    /// The composed <c>Content-Security-Policy</c> header value, built once at
    /// construction so the per-response path allocates nothing. Equals the
    /// baseline <see cref="ExplorerSecurityHeaders.ContentSecurityPolicyValue"/>
    /// when no provider contributed extra <c>form-action</c> sources; otherwise it
    /// is the baseline extended with those sources (see
    /// <see cref="ExplorerContentSecurityPolicyOptions"/>).
    /// </summary>
    private readonly StringValues _contentSecurityPolicy;

    /// <summary>
    /// Initializes the middleware and composes the cached
    /// <c>Content-Security-Policy</c> value from the baseline policy plus any
    /// <see cref="ExplorerContentSecurityPolicyOptions.AdditionalFormActionSources"/>
    /// a federated sign-out provider contributed. The composition happens once
    /// here (the middleware is a pipeline singleton), so no request pays for it.
    /// </summary>
    /// <param name="next">The next middleware in the pipeline.</param>
    /// <param name="cspOptions">
    /// The accumulated CSP source contributions, or <see langword="null"/> when
    /// the options infrastructure is absent (a bare unit-test construction), in
    /// which case the baseline policy is used.
    /// </param>
    public ExplorerSecurityHeadersMiddleware(
        RequestDelegate next,
        IOptions<ExplorerContentSecurityPolicyOptions>? cspOptions = null)
    {
        ArgumentNullException.ThrowIfNull(next);
        _next = next;

        var extraSources = cspOptions?.Value.AdditionalFormActionSources;
        _contentSecurityPolicy = extraSources is { Count: > 0 }
            ? ExplorerSecurityHeaders.BuildContentSecurityPolicy(extraSources)
            : ExplorerSecurityHeaders.ContentSecurityPolicy;
    }

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
            headers.ContentSecurityPolicy = _contentSecurityPolicy;
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

        return _next(context);
    }
}
