using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Drives the registered health checks on a background cadence and records each
/// report into <see cref="RepoContextHealthSignal"/>, from where
/// <see cref="RepoContextHealthMeter"/> publishes it onto <c>/metrics</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The cadence is the point, not an implementation detail.</b> Every health
/// component here is evaluated on demand, so before this publisher existed the
/// grain-liveness check - the one component that reads the reserved
/// <c>sys-auth-policy</c> tree, and therefore the one that can observe the wedge of
/// issue #2868 - ran only when something probed it over HTTP. A deployment with no
/// orchestrator probe, or one whose probe is pointed at <c>/metrics</c> because that
/// is the only exposed port, never exercised the authorization seam at all. Running
/// the checks on a timer means the seam is exercised whether or not anything asks,
/// and the answer is on the scrape either way.
/// </para>
/// <para>
/// <b>It records, and does nothing else.</b> It takes no remediating action on an
/// unhealthy verdict. Restarting a container whose authorization tree has wedged is
/// a decision for the deployment, not for the process that is itself wedged, and the
/// recovery assessment in the container guide sets out why that is not safely
/// automated from in here.
/// </para>
/// <para>
/// <b>It never throws into the health-check service.</b> A publisher that faults is
/// logged and dropped by the runtime, which would silently stop the series
/// advancing while the exposition still looked complete. Recording is therefore
/// wrapped, and a failure to record is itself visible: the component's evaluation
/// count stops rising, which the counter's own description tells a reader to check.
/// </para>
/// </remarks>
/// <param name="signal">The holder every report is recorded into.</param>
public sealed class RepoContextHealthPublisher(RepoContextHealthSignal signal) : IHealthCheckPublisher
{
    private readonly RepoContextHealthSignal _signal = signal
        ?? throw new ArgumentNullException(nameof(signal));

    /// <inheritdoc />
    public Task PublishAsync(HealthReport report, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(report);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.CompletedTask;
        }

        _signal.Publish(report);
        return Task.CompletedTask;
    }
}
