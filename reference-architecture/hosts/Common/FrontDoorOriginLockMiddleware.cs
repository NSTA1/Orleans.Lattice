using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// ASP.NET Core middleware that enforces the Azure Front Door origin lock. When a
/// non-empty Front Door id is configured, every inbound request (other than an
/// exempt path) must carry an <c>X-Azure-FDID</c> header whose value matches that
/// id; otherwise the request is rejected with HTTP 403.
/// </summary>
/// <remarks>
/// Azure Front Door stamps <c>X-Azure-FDID: &lt;frontDoorId&gt;</c> on every request
/// it forwards to an origin. A caller that reaches a per-region ingress FQDN
/// directly - bypassing the global Front Door and its WAF / routing tier - does not
/// carry the header and is refused. Internal platform probes (health, metrics, the
/// scaling signal) reach the container directly without transiting Front Door, so
/// their paths are registered as exempt by the host that mounts the middleware.
/// The header value is compared case-insensitively because the Front Door id is a
/// GUID whose textual casing is not significant.
/// </remarks>
public sealed class FrontDoorOriginLockMiddleware
{
    /// <summary>The header Azure Front Door stamps on every request it forwards to an origin.</summary>
    public const string FrontDoorIdHeaderName = "X-Azure-FDID";

    private readonly RequestDelegate _next;
    private readonly string _frontDoorId;
    private readonly string[] _exemptPathPrefixes;

    /// <summary>Initializes the middleware with the expected Front Door id and the exempt path prefixes.</summary>
    /// <param name="next">The next delegate in the request pipeline.</param>
    /// <param name="frontDoorId">The expected Front Door id; must be non-empty.</param>
    /// <param name="exemptPathPrefixes">Path prefixes that bypass the origin lock (e.g. internal probe endpoints).</param>
    public FrontDoorOriginLockMiddleware(RequestDelegate next, string frontDoorId, string[] exemptPathPrefixes)
    {
        ArgumentNullException.ThrowIfNull(next);
        ArgumentException.ThrowIfNullOrWhiteSpace(frontDoorId);
        ArgumentNullException.ThrowIfNull(exemptPathPrefixes);

        _next = next;
        _frontDoorId = frontDoorId;
        _exemptPathPrefixes = exemptPathPrefixes;
    }

    /// <summary>Rejects the request with HTTP 403 unless it is exempt or carries a matching <c>X-Azure-FDID</c> header.</summary>
    /// <param name="context">The current HTTP context.</param>
    public async Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!IsExempt(context.Request.Path))
        {
            // Require exactly one header value that matches the configured id. An
            // absent header (Count == 0), a duplicated header (Count > 1), or a
            // mismatched value all fail closed with 403.
            var header = context.Request.Headers[FrontDoorIdHeaderName];
            if (header.Count != 1 || !string.Equals(header[0], _frontDoorId, StringComparison.OrdinalIgnoreCase))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
        }

        await _next(context);
    }

    private bool IsExempt(PathString path)
    {
        foreach (var prefix in _exemptPathPrefixes)
        {
            if (path.StartsWithSegments(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
