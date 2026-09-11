using System.Globalization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The grain-liveness health check that backs the container's Docker healthcheck
/// (<c>/health/silo</c>). Unlike the always-green
/// <see cref="RepoContextLivenessHealthCheck"/> and the one-shot latched
/// <see cref="RepoContextReadinessHealthCheck"/>, it re-exercises the silo and
/// grain layer on <b>every</b> probe, so it goes red the moment a silo that had
/// reached readiness dies or wedges - the outage recorded by issue #2666, where
/// the process kept listening and every health surface stayed green.
/// </summary>
/// <remarks>
/// <para>
/// It is deliberately <b>three-valued</b>, and the distinction is load-bearing:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Healthy</b> - the trivial grain call completed, so membership is active and
/// the grain layer answers.
/// </description></item>
/// <item><description>
/// <b>Degraded (starting)</b> - the grain call has not yet succeeded and the host
/// has never reached readiness, so the silo is still joining. This is <i>not</i> a
/// fault: conflating it with unhealthy during normal boot, under the compose
/// service's <c>restart: unless-stopped</c>, is exactly what produces a startup
/// crash loop. The Docker healthcheck's <c>start_period</c> holds a Degraded result
/// in "starting" rather than "unhealthy".
/// </description></item>
/// <item><description>
/// <b>Unhealthy</b> - the grain call failed or timed out <i>after</i> the host had
/// reached readiness, so a working silo has since died or wedged.
/// </description></item>
/// </list>
/// <para>
/// A drain is reported Healthy without probing: the silo is stopping on purpose,
/// and grain calls failing as activations deactivate must not mark a gracefully
/// stopping container unhealthy.
/// </para>
/// </remarks>
/// <param name="probe">The seam that performs the trivial grain call.</param>
/// <param name="readiness">The shared lifecycle-phase holder.</param>
public sealed class RepoContextSiloHealthCheck(
    IRepoContextSiloProbe probe,
    RepoContextReadinessState readiness) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "silo";

    /// <summary>
    /// The default bound on a single grain-liveness call. A wedged silo answers by
    /// hanging rather than throwing, so the probe must impose its own deadline; this
    /// is comfortably under the Docker healthcheck's own <c>timeout</c> so a hang
    /// surfaces as a fast Unhealthy rather than a probe that never returns.
    /// </summary>
    public static readonly TimeSpan DefaultProbeTimeout = TimeSpan.FromSeconds(5);

    private readonly IRepoContextSiloProbe _probe = probe
        ?? throw new ArgumentNullException(nameof(probe));

    private readonly RepoContextReadinessState _readiness = readiness
        ?? throw new ArgumentNullException(nameof(readiness));

    private readonly TimeSpan _timeout = DefaultProbeTimeout;

    /// <summary>Test-only constructor allowing the probe timeout to be shortened.</summary>
    internal RepoContextSiloHealthCheck(
        IRepoContextSiloProbe probe,
        RepoContextReadinessState readiness,
        TimeSpan timeout)
        : this(probe, readiness)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "The probe timeout must be positive.");
        }

        _timeout = timeout;
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var phase = _readiness.Phase;

        // A draining container is stopping deliberately. Probing now would race the
        // teardown and a failing grain call is expected, not a fault, so report
        // healthy rather than mark a gracefully stopping box unhealthy in its last
        // seconds.
        if (phase == RepoContextLifecyclePhase.Draining)
        {
            return HealthCheckResult.Healthy(
                "Draining: graceful shutdown in progress; the silo is stopping on purpose.");
        }

        string failure;
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_timeout);
            await _probe.ProbeAsync(timeoutCts.Token).ConfigureAwait(false);
            return HealthCheckResult.Healthy(
                "Silo membership active and the grain layer answered a trivial call.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The probe request itself is being torn down (the caller went away), not
            // our own timeout. Surface it rather than misreport it as a silo fault.
            throw;
        }
        catch (OperationCanceledException)
        {
            failure = string.Create(
                CultureInfo.InvariantCulture,
                $"the grain call did not complete within {_timeout.TotalSeconds:F0}s, so the silo is wedged or unreachable");
        }
        catch (Exception ex)
        {
            failure = ex.Message;
        }

        // The grain call did not succeed. Whether that is a fault turns entirely on
        // whether the host was ever ready: a silo that has not yet joined is still
        // starting, and only a silo that HAD joined and no longer answers is broken.
        return phase == RepoContextLifecyclePhase.Starting
            ? HealthCheckResult.Degraded(
                $"Starting: the silo has not yet answered a grain call ({failure}).")
            : HealthCheckResult.Unhealthy(
                $"The silo is not answering grain calls ({failure}).");
    }
}

/// <summary>
/// The response writer for the <c>/health/silo</c> endpoint. It emits a single
/// plain-text line, <c>&lt;Status&gt;: &lt;description&gt;</c>, so the shell-less
/// container's <c>--healthcheck</c> self-probe can both classify the three-way
/// verdict and echo the reason into the Docker health log an operator reads with
/// <c>docker inspect</c> - the surface that was empty when issue #2666's outage had
/// to be found by eye.
/// </summary>
public static class RepoContextSiloHealthResponse
{
    /// <summary>Writes the one-line verdict for the single silo health entry.</summary>
    /// <param name="context">The HTTP context.</param>
    /// <param name="report">The health report (a single silo entry).</param>
    public static Task Write(HttpContext context, HealthReport report)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(report);

        string? description = null;
        foreach (var entry in report.Entries)
        {
            description = entry.Value.Description;
            break;
        }

        context.Response.ContentType = "text/plain; charset=utf-8";
        return context.Response.WriteAsync(
            $"{report.Status}: {description ?? report.Status.ToString()}");
    }
}

/// <summary>
/// Builds the <c>/health/silo</c> endpoint options - the tag predicate, the
/// three-way status-code mapping, and the plain-text response writer - in one place
/// so the host wiring and its tests map the endpoint identically. Extracted so a
/// test cannot pass against a mapping that has drifted from the one the host serves.
/// </summary>
public static class RepoContextSiloHealthEndpoint
{
    /// <summary>
    /// Creates the health-check options for the silo endpoint. Degraded (the silo
    /// still starting) and Unhealthy both map to HTTP 503 so a plain success check
    /// treats either as failure; the three-way verdict is carried in the body, which
    /// the shell-less self-probe classifies.
    /// </summary>
    /// <param name="siloTag">The tag identifying the silo health check.</param>
    public static HealthCheckOptions CreateOptions(string siloTag)
    {
        ArgumentException.ThrowIfNullOrEmpty(siloTag);

        return new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(siloTag),
            ResultStatusCodes = new Dictionary<HealthStatus, int>
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status503ServiceUnavailable,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
            },
            ResponseWriter = RepoContextSiloHealthResponse.Write,
        };
    }
}
