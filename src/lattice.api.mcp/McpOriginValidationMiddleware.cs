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

        var origins = request.Headers.Origin;
        if (origins.Count == 0)
        {
            return _next(context);
        }

        // A request carrying several Origin values is malformed; every one must
        // be allowed, so an attacker cannot smuggle a hostile origin past an
        // allowed one.
        foreach (var origin in origins)
        {
            if (!IsAllowed(origin))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return Task.CompletedTask;
            }
        }

        return _next(context);
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
