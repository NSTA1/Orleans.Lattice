using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes the container's own health verdict onto the existing <c>/metrics</c>
/// scrape, so the surface an operator, a dashboard and an orchestrator actually read
/// carries the same answer the Docker health log does.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this fixes.</b> Issue #2868 recorded 43 minutes during which
/// <c>docker inspect</c> reported <c>Health=unhealthy</c> - correctly, because the
/// grain-liveness probe reads the reserved <c>sys-auth-policy</c> tree and that tree
/// was wedged - while <c>/metrics</c> answered HTTP 200 with a complete scrape and
/// MCP returned 500 to every call needing authorization. The detection already
/// worked. What was missing is that its verdict reached no consumer: the scrape,
/// which is the only externally reachable signal and the only one anything is wired
/// to, carried no health series whatsoever.
/// </para>
/// <para>
/// <b>No new endpoint is added, deliberately.</b> A second health port is a thing
/// somebody has to wire up, and the deployment that most needs it is the one that
/// will not. The verdict is instead put on the endpoint that is already scraped.
/// </para>
/// <para>
/// <b>The scrape still answers 200, also deliberately.</b> Failing the scrape when
/// the box is wedged would destroy the telemetry at the exact moment it is needed to
/// explain the wedge, and would make the metrics path depend on the grain layer it
/// is currently independent of. Independence is the property that kept these series
/// available during the outage; it is preserved, and the verdict rides along inside
/// the payload instead.
/// </para>
/// <para>
/// <b>Every instrument is observable and reports its whole product on every
/// observation.</b> An instrument created on a first occurrence does not exist
/// during the window an alert is meant to cover, and a series first created late can
/// be refused outright at one of the collector's ceilings while the exposition still
/// looks complete (issue #2480). Enumerating the full component-by-status product,
/// and all five fault causes, on each sample means every arm exists from process
/// start and a zero is a measurement rather than a missing series.
/// </para>
/// </remarks>
public sealed class RepoContextHealthMeter : IDisposable
{
    /// <summary>
    /// The gauge carrying each health component's current verdict as a one-hot
    /// indicator over the <c>status</c> dimension.
    /// </summary>
    public const string StatusGaugeName = "lattice_repocontext_health_status";

    /// <summary>
    /// The counter reporting how many verdicts have been published per component. It
    /// is the denominator that makes a zero on <see cref="StatusGaugeName"/>
    /// interpretable.
    /// </summary>
    public const string EvaluationsCounterName = "lattice_repocontext_health_evaluations_total";

    /// <summary>
    /// The counter attributing grain-liveness probe failures to a bounded cause.
    /// </summary>
    public const string SiloProbeFaultsCounterName = "lattice_repocontext_silo_probe_faults_total";

    /// <summary>The tag naming the health component a measurement belongs to.</summary>
    public const string ComponentTagKey = "component";

    /// <summary>The tag naming which verdict a status measurement indicates.</summary>
    public const string StatusTagKey = "status";

    /// <summary>The tag naming why a grain-liveness probe failed.</summary>
    public const string CauseTagKey = "cause";

    /// <summary>The <see cref="HealthStatus.Healthy"/> value of <see cref="StatusTagKey"/>.</summary>
    public const string StatusHealthyTag = "healthy";

    /// <summary>The <see cref="HealthStatus.Degraded"/> value of <see cref="StatusTagKey"/>.</summary>
    public const string StatusDegradedTag = "degraded";

    /// <summary>The <see cref="HealthStatus.Unhealthy"/> value of <see cref="StatusTagKey"/>.</summary>
    public const string StatusUnhealthyTag = "unhealthy";

    /// <summary>The <see cref="RepoContextSiloProbeFaultCause.ProbeDeadline"/> value of <see cref="CauseTagKey"/>.</summary>
    public const string CauseProbeDeadlineTag = "probe-deadline";

    /// <summary>The <see cref="RepoContextSiloProbeFaultCause.GrainTimeout"/> value of <see cref="CauseTagKey"/>.</summary>
    public const string CauseGrainTimeoutTag = "grain-timeout";

    /// <summary>The <see cref="RepoContextSiloProbeFaultCause.AccessDenied"/> value of <see cref="CauseTagKey"/>.</summary>
    public const string CauseAccessDeniedTag = "access-denied";

    /// <summary>The <see cref="RepoContextSiloProbeFaultCause.DrainHung"/> value of <see cref="CauseTagKey"/>.</summary>
    public const string CauseDrainHungTag = "drain-hung";

    /// <summary>The <see cref="RepoContextSiloProbeFaultCause.Unexpected"/> value of <see cref="CauseTagKey"/>.</summary>
    public const string CauseUnexpectedTag = "unexpected";

    // Tags resolved through DescribeStatus rather than repeated as literals, so the
    // status-to-tag mapping has one home and the fixture that asserts it is asserting
    // what the scrape carries.
    private static readonly (HealthStatus Status, string Tag)[] StatusArms =
    [
        (HealthStatus.Healthy, DescribeStatus(HealthStatus.Healthy)),
        (HealthStatus.Degraded, DescribeStatus(HealthStatus.Degraded)),
        (HealthStatus.Unhealthy, DescribeStatus(HealthStatus.Unhealthy)),
    ];

    // Declared above every instrument it constructs, and every instrument is built
    // from this field, so a reordering throws at initialisation rather than
    // publishing against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;

    private readonly RepoContextHealthSignal _signal;

    /// <summary>Creates the meter and publishes every instrument.</summary>
    /// <param name="signal">The live health signal this meter reports.</param>
    /// <exception cref="ArgumentNullException"><paramref name="signal"/> is null.</exception>
    public RepoContextHealthMeter(RepoContextHealthSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        _signal = signal;

        // Published on the shared host meter, whose name sits under the collector's
        // subscribed prefix, so these series reach the existing /metrics endpoint
        // with no exposition change.
        _meter = new Meter(RepoContextHostMeter.Name);

        _meter.CreateObservableGauge(
            StatusGaugeName,
            ObserveStatus,
            unit: "{component}",
            description:
                "The container's own health verdict, one series per component and status, carrying 1 on "
                + "the component's current verdict and 0 on the other two. Alert on "
                + StatusGaugeName
                + "{status=\"unhealthy\"} == 1. A component that has never been evaluated carries 0 on ALL "
                + "THREE arms, which is not the same as healthy: read it against "
                + EvaluationsCounterName
                + ", where a zero means no verdict has been published yet. The full component-by-status "
                + "product is reported on every scrape, so a zero here is always a measurement and an "
                + "absent series can only mean the host did not construct this meter or the collector "
                + "refused the series at a ceiling. This exists because issue #2868 recorded a container "
                + "whose health check correctly reported unhealthy for 43 minutes while this endpoint "
                + "answered 200 with no health series at all, so the verdict reached no consumer.");

        _meter.CreateObservableCounter(
            EvaluationsCounterName,
            ObserveEvaluations,
            unit: "{evaluation}",
            description:
                "Health verdicts published per component since process start, counted on the background "
                + "publisher's cadence rather than on external HTTP probes. It denominates "
                + StatusGaugeName
                + ": an all-zero status block beside a zero here means the publisher has not run, while "
                + "an all-zero status block beside a positive count could only be an export defect. "
                + "Without it, 'the component is healthy' and 'nothing has evaluated the component' "
                + "render identically, which is the exact ambiguity issue #2868 was lost to. A rising "
                + "count is also positive proof the authorization seam is being exercised, because the "
                + "grain-liveness component reads the reserved sys-auth-policy tree on every evaluation.");

        _meter.CreateObservableCounter(
            SiloProbeFaultsCounterName,
            ObserveSiloFaults,
            unit: "{fault}",
            description:
                "Grain-liveness probe failures since process start, attributed to a bounded cause: "
                + "probe-deadline (the call hung past the check's own deadline, which is the wedged "
                + "authorization tree of issue #2868), grain-timeout (a downstream timeout surfaced), "
                + "access-denied (the tree answered and refused the grant, which a restart does not "
                + "clear), drain-hung (a graceful shutdown outran its stop-grace window), and unexpected "
                + "(a failure this taxonomy does not name). All five arms are published at zero from "
                + "process start, so a zero is a measured absence. It counts probe FAILURES and so "
                + "deliberately disagrees with "
                + StatusGaugeName
                + " during startup, where a failing probe is graded degraded rather than unhealthy: a "
                + "silo still joining quietly and one joining while failing every probe are otherwise "
                + "indistinguishable. It shares the publisher cadence with "
                + EvaluationsCounterName
                + ", so it can never exceed that component's evaluation count.");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private IEnumerable<Measurement<int>> ObserveStatus()
    {
        var components = _signal.Components;
        var measurements = new List<Measurement<int>>(components.Count * StatusArms.Length);

        foreach (var component in components)
        {
            var reading = _signal.Read(component);
            foreach (var (status, tag) in StatusArms)
            {
                measurements.Add(new Measurement<int>(
                    reading.Status == status ? 1 : 0,
                    new KeyValuePair<string, object?>(ComponentTagKey, component),
                    new KeyValuePair<string, object?>(StatusTagKey, tag)));
            }
        }

        return measurements;
    }

    private IEnumerable<Measurement<long>> ObserveEvaluations()
    {
        var components = _signal.Components;
        var measurements = new List<Measurement<long>>(components.Count);

        foreach (var component in components)
        {
            measurements.Add(new Measurement<long>(
                _signal.Read(component).Evaluations,
                new KeyValuePair<string, object?>(ComponentTagKey, component)));
        }

        return measurements;
    }

    private IEnumerable<Measurement<long>> ObserveSiloFaults()
    {
        var tally = _signal.ReadSiloFaults();

        // Rendered through DescribeCause rather than against tag literals, so the
        // enum-to-tag mapping has exactly one home and a fixture asserting that
        // mapping is asserting what the scrape actually carries.
        return
        [
            Fault(tally.ProbeDeadline, RepoContextSiloProbeFaultCause.ProbeDeadline),
            Fault(tally.GrainTimeout, RepoContextSiloProbeFaultCause.GrainTimeout),
            Fault(tally.AccessDenied, RepoContextSiloProbeFaultCause.AccessDenied),
            Fault(tally.DrainHung, RepoContextSiloProbeFaultCause.DrainHung),
            Fault(tally.Unexpected, RepoContextSiloProbeFaultCause.Unexpected),
        ];

        static Measurement<long> Fault(long value, RepoContextSiloProbeFaultCause cause) => new(
            value,
            new KeyValuePair<string, object?>(CauseTagKey, DescribeCause(cause)));
    }

    /// <summary>
    /// Renders a fault cause as its stable tag value, failing open onto
    /// <see cref="CauseUnexpectedTag"/> so an enum member added without a tag is
    /// reported rather than crashing the observation callback.
    /// </summary>
    /// <param name="cause">The cause to render.</param>
    public static string DescribeCause(RepoContextSiloProbeFaultCause cause) => cause switch
    {
        RepoContextSiloProbeFaultCause.ProbeDeadline => CauseProbeDeadlineTag,
        RepoContextSiloProbeFaultCause.GrainTimeout => CauseGrainTimeoutTag,
        RepoContextSiloProbeFaultCause.AccessDenied => CauseAccessDeniedTag,
        RepoContextSiloProbeFaultCause.DrainHung => CauseDrainHungTag,
        _ => CauseUnexpectedTag,
    };

    /// <summary>Renders a health status as its stable tag value.</summary>
    /// <param name="status">The status to render.</param>
    public static string DescribeStatus(HealthStatus status) => status switch
    {
        HealthStatus.Healthy => StatusHealthyTag,
        HealthStatus.Degraded => StatusDegradedTag,
        _ => StatusUnhealthyTag,
    };
}
