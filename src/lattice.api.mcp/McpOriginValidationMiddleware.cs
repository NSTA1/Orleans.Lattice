using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Refuses, with <c>403</c>, a request within the MCP transport path whose
/// <c>Origin</c> header names an origin that is not allowed, as the MCP
/// streamable-HTTP transport specification requires to prevent DNS-rebinding
/// attacks. A request without an <c>Origin</c> header passes. Inert (a single
/// per-request field check) when
/// <see cref="LatticeApiMcpOptions.RequireAllowedOrigin"/> is
/// <see langword="false"/>.
/// </summary>
internal sealed class McpOriginValidationMiddleware
{
    private readonly RequestDelegate _next;
    private readonly bool _enabled;
    private readonly bool _allowLoopback;
    private readonly PathString _transportPrefix;
    private readonly HashSet<string> _allowedOrigins;

    public McpOriginValidationMiddleware(RequestDelegate next, IOptions<LatticeApiMcpOptions> options)
    {
        _next = next;
        var value = options.Value;
        _enabled = value.RequireAllowedOrigin;
        _allowLoopback = value.AllowLoopbackOrigins;
        _transportPrefix = new PathString(ProtectedResourceMetadataChallengeMiddleware.NormalizePrefix(value.TransportPattern));
        _allowedOrigins = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var origin in value.AllowedOrigins)
        {
            if (!string.IsNullOrWhiteSpace(origin))
            {
                _allowedOrigins.Add(origin.Trim().TrimEnd('/'));
            }
        }
    }

    public Task InvokeAsync(HttpContext context)
    {
        if (!_enabled)
        {
            return _next(context);
        }

        var request = context.Request;
        if (_transportPrefix.HasValue
            && !request.Path.StartsWithSegments(_transportPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return _next(context);
        }

        if (!PermitsOrigin(request))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return Task.CompletedTask;
        }

        return _next(context);
    }

    /// <summary>
    /// Path-agnostic origin verdict for one request: <see langword="true"/> when
    /// the request carries no <c>Origin</c> header or every value it carries is
    /// allowed. The transport endpoint applies this itself, so the guard still
    /// holds when the host mounts MCP under a path base or a route group that the
    /// outermost middleware's path prefix does not see.
    /// </summary>
    internal bool PermitsOrigin(HttpRequest request)
    {
        // A request carrying several Origin values is malformed; every one must
        // be allowed, so an attacker cannot smuggle a hostile origin past an
        // allowed one.
        foreach (var origin in request.Headers.Origin)
        {
            if (!IsAllowed(origin))
            {
                return false;
            }
        }

        return true;
    }

    internal bool IsAllowed(string? origin)
    {
        if (string.IsNullOrWhiteSpace(origin))
        {
            return false;
        }

        if (_allowedOrigins.Contains(origin))
        {
            return true;
        }

        return _allowLoopback && IsLoopbackOrigin(origin);
    }

    internal static bool IsLoopbackOrigin(string origin)
    {
        if (!Uri.TryCreate(origin, UriKind.Absolute, out var uri))
        {
            return false;
        }

        if (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)
        {
            return false;
        }

        // An origin is scheme://host[:port] only; anything else is not a
        // well-formed origin and is not trusted.
        if (!string.IsNullOrEmpty(uri.UserInfo) || uri.AbsolutePath != "/" || uri.Query.Length > 0 || uri.Fragment.Length > 0)
        {
            return false;
        }

        if (string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return IPAddress.TryParse(uri.IdnHost.Trim('[', ']'), out var address) && IPAddress.IsLoopback(address);
    }
}
