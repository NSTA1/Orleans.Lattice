using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Why the grain-liveness probe (<see cref="RepoContextSiloHealthCheck"/>) did not
/// complete. The taxonomy is deliberately small and closed, so every arm can be
/// published at zero from process start and a zero therefore reads as a measured
/// absence rather than as a series nobody has created yet.
/// </summary>
/// <remarks>
/// <para>
/// <b>The remedies differ, which is why the distinction is drawn at all.</b> A
/// <see cref="ProbeDeadline"/> is the wedge shape recorded by issue #2868: the grain
/// call neither completed nor threw, so the authorization tree is not answering and
/// only a restart has been observed to clear it. An <see cref="AccessDenied"/> is a
/// grant defect on a perfectly healthy tree and a restart would not touch it. Both
/// present identically on every surface that reports only <i>that</i> the probe
/// failed, and acting on the wrong one is how a healthy box gets restarted or a
/// wedged one gets left alone.
/// </para>
/// </remarks>
public enum RepoContextSiloProbeFaultCause
{
    /// <summary>
    /// No fault: the probe completed, or the container is draining inside its grace
    /// window, where a failing grain call is expected rather than a defect.
    /// </summary>
    None = 0,

    /// <summary>
    /// The check's own deadline fired: the grain call hung rather than returning or
    /// throwing. This is the shape issue #2868 recorded, where a wedged
    /// <c>sys-auth-policy</c> tree left the authorization path unanswerable.
    /// </summary>
    ProbeDeadline = 1,

    /// <summary>
    /// A <see cref="TimeoutException"/> surfaced from the grain call itself, so the
    /// call was refused on a downstream deadline rather than on the probe's own.
    /// </summary>
    GrainTimeout = 2,

    /// <summary>
    /// The access gate refused the probe. The tree is answering; the grant is wrong.
    /// A restart does not clear this.
    /// </summary>
    AccessDenied = 3,

    /// <summary>
    /// A graceful drain has run beyond its stop-grace window and has not completed,
    /// so the shutdown itself has hung.
    /// </summary>
    DrainHung = 4,

    /// <summary>
    /// The probe failed in a way this taxonomy does not name. It exists so that a
    /// future failure path which classifies nothing still increments a series
    /// instead of disappearing, which is the failure mode this whole signal exists
    /// to end.
    /// </summary>
    Unexpected = 5,
}

/// <summary>
/// The per-cause count of grain-liveness probe failures, plus the independently
/// maintained <see cref="Total"/> they must sum to.
/// </summary>
/// <remarks>
/// <see cref="Total"/> is not computed from the arms. It is incremented on the same
/// path and compared against their sum by a fixture, so an arm that is recorded
/// nowhere, or recorded twice, is a build failure rather than a quiet discrepancy on
/// a scrape.
/// </remarks>
/// <param name="ProbeDeadline">Failures classified <see cref="RepoContextSiloProbeFaultCause.ProbeDeadline"/>.</param>
/// <param name="GrainTimeout">Failures classified <see cref="RepoContextSiloProbeFaultCause.GrainTimeout"/>.</param>
/// <param name="AccessDenied">Failures classified <see cref="RepoContextSiloProbeFaultCause.AccessDenied"/>.</param>
/// <param name="DrainHung">Failures classified <see cref="RepoContextSiloProbeFaultCause.DrainHung"/>.</param>
/// <param name="Unexpected">Failures classified <see cref="RepoContextSiloProbeFaultCause.Unexpected"/>.</param>
/// <param name="Total">Every failure recorded, counted independently of the arms.</param>
public readonly record struct RepoContextSiloProbeFaultTally(
    long ProbeDeadline,
    long GrainTimeout,
    long AccessDenied,
    long DrainHung,
    long Unexpected,
    long Total)
{
    /// <summary>The sum of the five named arms.</summary>
    public long ArmSum => ProbeDeadline + GrainTimeout + AccessDenied + DrainHung + Unexpected;
}

/// <summary>
/// One component's last published verdict and how many verdicts it has published.
/// </summary>
/// <param name="Status">
/// The last verdict, or <c>null</c> when the component has never been evaluated.
/// </param>
/// <param name="Evaluations">Verdicts published for this component since process start.</param>
public readonly record struct RepoContextHealthReading(HealthStatus? Status, long Evaluations);

/// <summary>
/// The process-wide holder of the last health verdict for every registered
/// component, written by <see cref="RepoContextHealthPublisher"/> and read by
/// <see cref="RepoContextHealthMeter"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> Issue #2868 recorded a container that answered
/// <c>/metrics</c> with HTTP 200 and a full scrape for 43 minutes while every MCP
/// call needing authorization returned 500. The health check was not wrong - it
/// reported unhealthy throughout - but its verdict existed only in the Docker health
/// log. Every consumer that is actually wired up (Prometheus, dashboards, alert
/// rules, and any liveness probe pointed at the single exposed port) reads the
/// scrape, and the scrape carried no health series at all. So the correct verdict
/// was computed, and nothing could read it.
/// </para>
/// <para>
/// <b>The scrape deliberately still answers 200.</b> Making <c>/metrics</c> fail
/// when the box is wedged would delete the telemetry at the exact moment it is
/// needed to diagnose the wedge. The metrics endpoint is a pure in-process render of
/// the collector's aggregated state and makes no grain call, which is why it stayed
/// up: it is genuinely independent of the grain layer rather than merely not
/// exercising it. That independence is worth keeping. What changes here is that the
/// reassuring signal now <i>carries</i> the damning one.
/// </para>
/// <para>
/// <b>Never-evaluated is not healthy.</b> A component that has published no verdict
/// reports <c>null</c>, which the meter renders as zero on every status arm, and its
/// evaluation counter stays at zero beside it. That pair is what makes an all-zero
/// status block interpretable: zero evaluations means the publisher has not run,
/// while a positive evaluation count with no status arm set could only be an export
/// defect. Defaulting an unevaluated component to healthy would reproduce, one level
/// up, the false green this type exists to remove.
/// </para>
/// </remarks>
public sealed class RepoContextHealthSignal
{
    /// <summary>The registration name of the grain-liveness component whose faults are attributed.</summary>
    public const string SiloComponent = RepoContextSiloHealthCheck.Name;

    private readonly Lock _gate = new();
    private readonly string[] _components;
    private readonly Dictionary<string, HealthStatus?> _status;
    private readonly Dictionary<string, long> _evaluations;
    private readonly Dictionary<RepoContextSiloProbeFaultCause, long> _faults;
    private long _faultTotal;

    /// <summary>
    /// Creates the holder over a fixed set of component names, every one of which is
    /// reported from construction so no series first appears on a first occurrence.
    /// </summary>
    /// <param name="components">
    /// The registered health-check names. Taken at construction rather than
    /// discovered from the first report, so a component that has never been
    /// evaluated is still published (at zero) instead of being absent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="components"/> is null.</exception>
    public RepoContextHealthSignal(IEnumerable<string> components)
    {
        ArgumentNullException.ThrowIfNull(components);

        _components = components
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        _status = new Dictionary<string, HealthStatus?>(StringComparer.Ordinal);
        _evaluations = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var component in _components)
        {
            _status[component] = null;
            _evaluations[component] = 0;
        }

        // Every fault arm is minted here rather than on a first occurrence, so a zero
        // on the scrape is a measurement and an absent series can only mean the meter
        // was never constructed.
        _faults = new Dictionary<RepoContextSiloProbeFaultCause, long>
        {
            [RepoContextSiloProbeFaultCause.ProbeDeadline] = 0,
            [RepoContextSiloProbeFaultCause.GrainTimeout] = 0,
            [RepoContextSiloProbeFaultCause.AccessDenied] = 0,
            [RepoContextSiloProbeFaultCause.DrainHung] = 0,
            [RepoContextSiloProbeFaultCause.Unexpected] = 0,
        };
    }

    /// <summary>The component names this signal reports, in registration order.</summary>
    public IReadOnlyList<string> Components => _components;

    /// <summary>
    /// Records one published health report: every entry's verdict, and the
    /// grain-liveness entry's fault cause when it carries one.
    /// </summary>
    /// <param name="report">The report produced by the health-check service.</param>
    /// <exception cref="ArgumentNullException"><paramref name="report"/> is null.</exception>
    public void Publish(HealthReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        lock (_gate)
        {
            foreach (var (name, entry) in report.Entries)
            {
                // A report may name a component registered after this holder was
                // built. Recording it would create a series mid-flight, which the
                // collector can refuse at a ceiling while the exposition still looks
                // complete, so an unknown component is ignored rather than added.
                if (!_status.ContainsKey(name))
                {
                    continue;
                }

                _status[name] = entry.Status;
                _evaluations[name] = _evaluations[name] + 1;

                if (!string.Equals(name, SiloComponent, StringComparison.Ordinal))
                {
                    continue;
                }

                var cause = ReadCause(entry);
                if (cause != RepoContextSiloProbeFaultCause.None)
                {
                    _faults[cause] = _faults[cause] + 1;

                    // Incremented on the same path but held independently of the arms,
                    // so a fixture can prove the two agree.
                    _faultTotal++;
                }
            }
        }
    }

    /// <summary>Reads one component's last verdict and evaluation count.</summary>
    /// <param name="component">The registered component name.</param>
    public RepoContextHealthReading Read(string component)
    {
        ArgumentException.ThrowIfNullOrEmpty(component);

        lock (_gate)
        {
            return _status.TryGetValue(component, out var status)
                ? new RepoContextHealthReading(status, _evaluations[component])
                : new RepoContextHealthReading(null, 0);
        }
    }

    /// <summary>Reads the per-cause grain-liveness fault counts and their independent total.</summary>
    public RepoContextSiloProbeFaultTally ReadSiloFaults()
    {
        lock (_gate)
        {
            return new RepoContextSiloProbeFaultTally(
                _faults[RepoContextSiloProbeFaultCause.ProbeDeadline],
                _faults[RepoContextSiloProbeFaultCause.GrainTimeout],
                _faults[RepoContextSiloProbeFaultCause.AccessDenied],
                _faults[RepoContextSiloProbeFaultCause.DrainHung],
                _faults[RepoContextSiloProbeFaultCause.Unexpected],
                _faultTotal);
        }
    }

    /// <summary>
    /// Extracts the bounded fault cause a health entry carries, failing open onto
    /// <see cref="RepoContextSiloProbeFaultCause.Unexpected"/> when a non-healthy
    /// entry classified nothing.
    /// </summary>
    /// <param name="entry">The grain-liveness report entry.</param>
    internal static RepoContextSiloProbeFaultCause ReadCause(HealthReportEntry entry)
    {
        if (entry.Data is { Count: > 0 }
            && entry.Data.TryGetValue(RepoContextSiloHealthCheck.CauseDataKey, out var raw)
            && raw is RepoContextSiloProbeFaultCause cause)
        {
            return cause;
        }

        // No classification. A healthy entry genuinely has no fault; anything else
        // failed without saying how, and that must still be counted rather than
        // silently dropped.
        return entry.Status == HealthStatus.Healthy
            ? RepoContextSiloProbeFaultCause.None
            : RepoContextSiloProbeFaultCause.Unexpected;
    }
}
