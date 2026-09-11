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
/// A drain is reported Healthy <b>while it stays within a bounded window</b>: the
/// silo is stopping on purpose, and grain calls failing as activations deactivate
/// must not mark a gracefully stopping container unhealthy. But a drain is not
/// unbounded - <see cref="RepoContextReadinessState.BeginDrain"/> is terminal, so a
/// host that begins graceful shutdown and then <b>hangs</b> would otherwise report
/// Healthy forever, which is issue #2401's abandoned drain wearing a health check
/// and the exact "alive, not serving, reporting green" shape issue #2666 exists to
/// end. Past the window the check reports Unhealthy and names the elapsed time. The
/// window is sized off the container's <c>stop_grace_period</c> (see
/// <see cref="DefaultDrainGraceWindow"/>), because that is the ceiling a
/// <c>docker stop</c> drain is killed at anyway; only a <i>self-initiated</i>
/// shutdown (a background failure calling <c>StopApplication</c>) can outlive it,
/// and that is precisely the case with no other backstop.
/// </para>
/// <para>
/// <b>The drain bound is observability, not remediation.</b> Docker does not
/// restart a container on an unhealthy result by itself, so reporting Unhealthy for
/// a hung drain does not recover it. What it buys is that a stuck drain becomes
/// <b>visible</b> in <c>docker ps</c> and <c>docker inspect</c> instead of having to
/// be found by eye - which is the whole complaint in issue #2666.
/// </para>
/// </remarks>
public sealed class RepoContextSiloHealthCheck : IHealthCheck
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

    /// <summary>
    /// The default bound on how long a graceful drain may run before it is reported
    /// Unhealthy as hung. It tracks <see cref="RepoContextShutdownBudget.DefaultStopGracePeriod"/>
    /// - the same <c>stop_grace_period</c> the shutdown budget is derived from - so a
    /// change to the container's grace period is visibly a change to both: a drain in
    /// the <c>docker stop</c> path is SIGKILLed at that grant regardless, so anything
    /// still draining past it can only be a self-initiated shutdown that has hung. The
    /// host wiring passes the <b>resolved</b> grant (which may be operator-overridden)
    /// through the DI factory; this default is what an unconfigured deployment uses.
    /// </summary>
    public static readonly TimeSpan DefaultDrainGraceWindow =
        RepoContextShutdownBudget.DefaultStopGracePeriod;

    private readonly IRepoContextSiloProbe _probe;
    private readonly RepoContextReadinessState _readiness;
    private readonly TimeSpan _drainGraceWindow;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _timeout;

    /// <summary>Initializes the check with the default drain window and system clock.</summary>
    /// <param name="probe">The seam that performs the trivial grain call.</param>
    /// <param name="readiness">The shared lifecycle-phase holder.</param>
    public RepoContextSiloHealthCheck(
        IRepoContextSiloProbe probe,
        RepoContextReadinessState readiness)
        : this(probe, readiness, DefaultDrainGraceWindow, TimeProvider.System, DefaultProbeTimeout)
    {
    }

    /// <summary>
    /// Initializes the check with an explicit drain-grace window and clock. The host
    /// wires this through a health-check factory so the window tracks the resolved
    /// <c>stop_grace_period</c> grant rather than the compile-time default.
    /// </summary>
    /// <param name="probe">The seam that performs the trivial grain call.</param>
    /// <param name="readiness">The shared lifecycle-phase holder.</param>
    /// <param name="drainGraceWindow">
    /// How long a graceful drain may run before it is reported Unhealthy as hung.
    /// </param>
    /// <param name="timeProvider">The clock the drain-duration bound is measured on.</param>
    public RepoContextSiloHealthCheck(
        IRepoContextSiloProbe probe,
        RepoContextReadinessState readiness,
        TimeSpan drainGraceWindow,
        TimeProvider timeProvider)
        : this(probe, readiness, drainGraceWindow, timeProvider, DefaultProbeTimeout)
    {
    }

    /// <summary>Test-only constructor allowing the probe timeout to be shortened.</summary>
    internal RepoContextSiloHealthCheck(
        IRepoContextSiloProbe probe,
        RepoContextReadinessState readiness,
        TimeSpan timeout)
        : this(probe, readiness, DefaultDrainGraceWindow, TimeProvider.System, timeout)
    {
    }

    private RepoContextSiloHealthCheck(
        IRepoContextSiloProbe probe,
        RepoContextReadinessState readiness,
        TimeSpan drainGraceWindow,
        TimeProvider timeProvider,
        TimeSpan timeout)
    {
        _probe = probe ?? throw new ArgumentNullException(nameof(probe));
        _readiness = readiness ?? throw new ArgumentNullException(nameof(readiness));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

        if (drainGraceWindow <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(drainGraceWindow), drainGraceWindow, "The drain grace window must be positive.");
        }

        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "The probe timeout must be positive.");
        }

        _drainGraceWindow = drainGraceWindow;
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
        // seconds - BUT only while the drain stays within its grace window. BeginDrain
        // is terminal, so a self-initiated shutdown that hangs would otherwise report
        // Healthy forever (issue #2401's abandoned drain as a health check). Past the
        // window we report Unhealthy naming the elapsed time. This is observability,
        // not remediation: Docker does not restart on unhealthy, so this only makes a
        // hung drain visible in `docker ps` / `docker inspect` rather than found by eye.
        if (phase == RepoContextLifecyclePhase.Draining)
        {
            var startedAt = _readiness.DrainStartedAtUtc;

            // A non-null start time is published before the Draining phase (see
            // BeginDrain), so a null here can only be a benign observation race with a
            // drain that has only just begun; treat it as just-started -> Healthy.
            if (startedAt is { } drainStart)
            {
                var elapsed = _timeProvider.GetUtcNow() - drainStart;
                if (elapsed > _drainGraceWindow)
                {
                    return HealthCheckResult.Unhealthy(
                        $"Draining: graceful shutdown has run {elapsed.TotalSeconds:F0}s, beyond the "
                        + $"{_drainGraceWindow.TotalSeconds:F0}s stop-grace window, and has not completed - "
                        + "the drain has hung.");
                }
            }

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
