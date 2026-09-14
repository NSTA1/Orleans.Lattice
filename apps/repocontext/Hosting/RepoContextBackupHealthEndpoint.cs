using Microsoft.AspNetCore.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Builds the <c>/health/backup</c> endpoint options - the tag predicate and the
/// plain-text response writer - in one place so the host wiring and its tests map the
/// endpoint identically, and a test cannot pass against a mapping that has drifted
/// from the one the host serves.
/// </summary>
/// <remarks>
/// <para>
/// <b>Issue #2980.</b> The mapping previously carried a bare tag predicate and no
/// response writer, so the framework default wrote the aggregate status word alone.
/// <see cref="RepoContextBackupHealthCheck"/> computes a three-valued verdict and a
/// description rendering which tree is in scope, how many backups the sink holds and
/// when the newest was taken, whether the last full capture described zero entries,
/// whether the captured scope disagrees with the configured scope, whether the sink is
/// non-durable, and the last failure text - and every probe threw all of it away.
/// That was measured off the wire before it was fixed: a live container returned
/// <c>503</c> with a body of exactly <c>Unhealthy</c>, eleven bytes including the
/// trailing newline.
/// </para>
/// <para>
/// The documentation had already been written against the intended behaviour.
/// <c>docs/lattice.api.mcp.repocontext/container.md</c> stated that the response body
/// "carries the full positive statement - which tree, how many entries, when, and the
/// last failure text". The content it described is real and is exactly what
/// <see cref="RepoContextBackupStatus.Describe"/> renders; the claim was false only
/// about the body, because the transport dropped it. A claim about a response body is
/// not verified by reading the check that produces it.
/// </para>
/// <para>
/// <b>This endpoint is not a liveness or readiness signal and is deliberately tagged
/// as neither.</b> A failing backup must not restart the container or pull it out of
/// rotation, because that converts a durability fault into an availability outage.
/// Making the body attributable does not change that: the verdict and the status code
/// are unmoved and only the description now reaches the wire.
/// </para>
/// </remarks>
public static class RepoContextBackupHealthEndpoint
{
    /// <summary>
    /// Creates the health-check options for the backup endpoint.
    /// </summary>
    /// <remarks>
    /// <see cref="HealthCheckOptions.ResultStatusCodes"/> is deliberately left at the
    /// framework default (Healthy and Degraded 200, Unhealthy 503), for the same
    /// reason as the readiness endpoint: this change adds detail to the body and must
    /// not move the verdict an operator or a scrape records.
    /// </remarks>
    /// <param name="backupTag">The tag identifying the backup-tagged health checks.</param>
    /// <returns>The options used to map the backup endpoint.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupTag"/> is null or empty.</exception>
    public static HealthCheckOptions CreateOptions(string backupTag)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupTag);

        return new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(backupTag),
            ResponseWriter = RepoContextComponentHealthResponse.Write,
    
        };
    }
}
