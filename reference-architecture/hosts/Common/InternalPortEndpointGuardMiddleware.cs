using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// ASP.NET Core middleware that confines a set of path prefixes to a single internal
/// Kestrel listener. A request whose accepted-socket local port is not the configured
/// internal port and whose path matches one of the guarded prefixes is answered with
/// HTTP 404, as though the endpoint were not mapped on that listener.
/// </summary>
/// <remarks>
/// A silo host binds Kestrel to two ports - an internal HTTP/1 port for platform
/// scrapers (metrics, the scaling signal, health) and an externally exposed HTTP/2
/// port for the gRPC facades - but ASP.NET Core endpoint routing answers a mapped
/// endpoint on every listener, not only the one it was intended for. Endpoints that
/// are meant to be internal-only therefore leak onto the external port. This
/// middleware closes that gap by gating on <see cref="ConnectionInfo.LocalPort"/>, the
/// port of the socket the connection was accepted on, which Kestrel sets from the
/// listener itself. Unlike the <c>Host</c> / <c>:authority</c> header it is not
/// supplied by the caller, so it cannot be spoofed from the external ingress. The
/// guarded paths return 404 (rather than 403) on any other listener so their existence
/// is not confirmed to an unauthenticated caller.
/// </remarks>
public sealed class InternalPortEndpointGuardMiddleware
{
    private readonly RequestDelegate _next;
    private readonly int _internalPort;
    private readonly string[] _internalOnlyPathPrefixes;

    /// <summary>Initializes the middleware with the internal listener port and the guarded path prefixes.</summary>
    /// <param name="next">The next delegate in the request pipeline.</param>
    /// <param name="internalPort">The internal Kestrel listener port the guarded paths are confined to; must be a valid TCP port.</param>
    /// <param name="internalOnlyPathPrefixes">Path prefixes that are answered only on <paramref name="internalPort"/> and return 404 on any other listener.</param>
    public InternalPortEndpointGuardMiddleware(RequestDelegate next, int internalPort, string[] internalOnlyPathPrefixes)
    {
        ArgumentNullException.ThrowIfNull(next);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(internalPort);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(internalPort, 65535);
        ArgumentNullException.ThrowIfNull(internalOnlyPathPrefixes);

        _next = next;
        _internalPort = internalPort;
        _internalOnlyPathPrefixes = internalOnlyPathPrefixes;
    }

    /// <summary>Answers a guarded path with HTTP 404 unless the request arrived on the internal listener port.</summary>
    /// <param name="context">The current HTTP context.</param>
    public async Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Connection.LocalPort != _internalPort && MatchesGuardedPath(context.Request.Path))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        await _next(context);
    }

    private bool MatchesGuardedPath(PathString path)
    {
        foreach (var prefix in _internalOnlyPathPrefixes)
        {
            if (path.StartsWithSegments(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
