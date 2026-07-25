using Microsoft.AspNetCore.Builder;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting;

/// <summary>
/// Extension methods that mount the Front Door origin lock on a reference-architecture host.
/// </summary>
public static class FrontDoorOriginLockApplicationBuilderExtensions
{
    /// <summary>The liveness probe path, always exempt from the origin lock because platform probes bypass Front Door.</summary>
    public const string HealthPath = "/health";

    /// <summary>
    /// Enforces the Azure Front Door origin lock when <paramref name="frontDoorId"/> is set and non-empty:
    /// any inbound request that is not an exempt path and does not carry a matching <c>X-Azure-FDID</c>
    /// header is rejected with HTTP 403. When <paramref name="frontDoorId"/> is null, empty, or whitespace
    /// the lock is disabled and the pipeline is left unchanged (the dev / compose harness and the first
    /// deployment pass, before Front Door exists, run unlocked).
    /// </summary>
    /// <param name="app">The application builder.</param>
    /// <param name="frontDoorId">The expected Front Door id (a GUID); empty disables the lock.</param>
    /// <param name="additionalExemptPathPrefixes">
    /// Extra path prefixes to exempt in addition to <see cref="HealthPath"/> (for example the silo's
    /// internal-only <c>/metrics</c> and scaling-signal endpoints, which platform scrapers reach directly).
    /// </param>
    /// <returns>The application builder, for chaining.</returns>
    public static IApplicationBuilder UseFrontDoorOriginLock(
        this IApplicationBuilder app,
        string? frontDoorId,
        params string[] additionalExemptPathPrefixes)
    {
        ArgumentNullException.ThrowIfNull(app);

        if (string.IsNullOrWhiteSpace(frontDoorId))
        {
            return app;
        }

        var exemptPathPrefixes = new List<string> { HealthPath };
        foreach (var prefix in additionalExemptPathPrefixes ?? [])
        {
            if (!string.IsNullOrWhiteSpace(prefix))
            {
                exemptPathPrefixes.Add(prefix);
            }
        }

        return app.UseMiddleware<FrontDoorOriginLockMiddleware>(frontDoorId, exemptPathPrefixes.ToArray());
    }
}
