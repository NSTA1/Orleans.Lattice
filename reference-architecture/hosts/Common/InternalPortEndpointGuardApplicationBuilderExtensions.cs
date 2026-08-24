using Microsoft.AspNetCore.Builder;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// Extension methods that confine internal-only endpoints to a single Kestrel listener
/// on a reference-architecture host.
/// </summary>
public static class InternalPortEndpointGuardApplicationBuilderExtensions
{
    /// <summary>
    /// Confines the given path prefixes to the internal <paramref name="internalPort"/> listener: a request for
    /// one of those paths that did not arrive on <paramref name="internalPort"/> is answered with HTTP 404. When
    /// no non-empty prefix is supplied the pipeline is left unchanged.
    /// </summary>
    /// <param name="app">The application builder.</param>
    /// <param name="internalPort">The internal Kestrel listener port the guarded paths are confined to.</param>
    /// <param name="internalOnlyPathPrefixes">
    /// The path prefixes to confine to <paramref name="internalPort"/> (for example the silo's <c>/metrics</c>
    /// and <c>/lattice/scale</c> endpoints, which are exempt from the Front Door origin lock and must not be
    /// reachable on the externally exposed gRPC listener).
    /// </param>
    /// <returns>The application builder, for chaining.</returns>
    public static IApplicationBuilder UseInternalPortEndpointGuard(
        this IApplicationBuilder app,
        int internalPort,
        params string[] internalOnlyPathPrefixes)
    {
        ArgumentNullException.ThrowIfNull(app);

        var guardedPrefixes = new List<string>();
        foreach (var prefix in internalOnlyPathPrefixes ?? [])
        {
            if (!string.IsNullOrWhiteSpace(prefix))
            {
                guardedPrefixes.Add(prefix);
            }
        }

        if (guardedPrefixes.Count == 0)
        {
            return app;
        }

        return app.UseMiddleware<InternalPortEndpointGuardMiddleware>(internalPort, guardedPrefixes.ToArray());
    }
}
