using Microsoft.AspNetCore.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Builds the <c>/health/ready</c> endpoint options - the tag predicate and the
/// plain-text response writer - in one place so the host wiring and its tests map the
/// endpoint identically, and a test cannot pass against a mapping that has drifted
/// from the one the host serves.
/// </summary>
/// <remarks>
/// <para>
/// <c>/health/ready</c> is the conjunction of
/// <see cref="RepoContextReadinessHealthCheck"/> (lifecycle) and
/// <see cref="RepoContextRetrievalReadinessHealthCheck"/> (vector plane), plus the
/// scaling-signal check on the Azure profile. Before issue #2962 the mapping carried a
/// bare predicate, so a captured body of <c>Unhealthy</c> could not distinguish a
/// still-replaying lifecycle from a degraded vector plane - two conditions with
/// entirely different remedies, only one of which is a retrieval finding.
/// </para>
/// </remarks>
public static class RepoContextReadinessHealthEndpoint
{
    /// <summary>
    /// Creates the health-check options for the readiness endpoint.
    /// </summary>
    /// <remarks>
    /// <see cref="HealthCheckOptions.ResultStatusCodes"/> is deliberately left at the
    /// framework default (Healthy and Degraded 200, Unhealthy 503). The status code is
    /// what an orchestrator routes on and what the acceptance predicate records, so it
    /// is held fixed here on purpose: this change adds detail to the body and must not
    /// move the verdict. <c>RepoContextReadinessHealthEndpointTests</c> pins the
    /// mapping so a later edit cannot change it quietly.
    /// </remarks>
    /// <param name="readinessTag">The tag identifying the readiness-tagged health checks.</param>
    /// <returns>The options used to map the readiness endpoint.</returns>
    /// <exception cref="ArgumentException"><paramref name="readinessTag"/> is null or empty.</exception>
    public static HealthCheckOptions CreateOptions(string readinessTag)
    {
        ArgumentException.ThrowIfNullOrEmpty(readinessTag);

        return new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(readinessTag),
            ResponseWriter = RepoContextComponentHealthResponse.Write,
        };
    }
}
