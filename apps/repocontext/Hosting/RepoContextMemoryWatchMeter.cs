using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes what this corpus actually required on this host - the peak commitment
/// reached, the share of the granted ceiling it consumed, and whether the ceiling was
/// ever exhausted - so the next deployment is sized from a measurement rather than
/// from constants fitted somewhere else.
/// </summary>
/// <remarks>
/// <para>
/// <b>Issue #3255, item 3.</b> The container's memory grant is a deploy-time absolute
/// produced by fitting a corpus model on one host, and nothing afterwards reports
/// what the corpus turned out to need. <see cref="RepoContextHeapCeilingMeter"/>
/// answers "how much is committed right now against the ceiling"; it does not answer
/// "how close did this deployment ever come", which is the question a sizing decision
/// actually turns on. An instantaneous gauge cannot answer it: the peak occurs during
/// ingest, and a scrape landing before or after it reports a comfortable figure that
/// says nothing about the margin that was really consumed.
/// </para>
/// <para>
/// <b>These are measurements this process took, not predictions.</b> Nothing here
/// contains a byte constant, a corpus model, or a fraction. The peak is a sampled
/// high-water mark and the ratio is that peak over the ceiling the runtime reported,
/// which is exactly the pairing <see cref="RepoContextHeapCeilingMeter"/> already
/// publishes instantaneously - the same two numbers, remembered at their worst.
/// </para>
/// <para>
/// <b>The ratio is derived from one reading, on purpose.</b> It could be divided in a
/// query, but only by joining two series scraped independently; deriving it here from
/// a single sample makes the numerator and denominator provably the same observation.
/// This follows <see cref="RepoContextHeapCeilingMeter.ReachableGaugeName"/>, which
/// exists for the same reason.
/// </para>
/// <para>
/// <b>Zero semantics.</b> Every instrument here is observable and therefore published
/// from process start with a real value, so absent and zero are distinguishable by
/// construction: an absent series means the host did not construct this meter, never
/// that nothing happened. A zero on
/// <see cref="ExhaustionEventsCounterName"/> is a measured absence of exhaustion so
/// far this run. <see cref="InsufficientLimitBytesGaugeName"/> is the one deliberate
/// exception, and it publishes <i>no measurement at all</i> rather than a zero when
/// nothing is recorded, because a zero there would read as "exhausted at a ceiling of
/// zero bytes".
/// </para>
/// <para>
/// <b>What the exhaustion counter can and cannot see.</b> It counts <i>managed</i>
/// <see cref="OutOfMemoryException"/> observed first-chance, which is the form the
/// documented failure takes - the exceptions are caught and surface as STORAGE errors
/// reading grain state, so nothing terminates and no unhandled-exception path sees
/// them. A cgroup out-of-memory kill is a different path entirely: it delivers
/// SIGKILL, raises no exception, and is counted here as zero. <b>A zero on this
/// counter is therefore evidence about managed exhaustion only and is not evidence
/// that memory was sufficient.</b> Read it beside the container's own restart count
/// and <c>OOMKilled</c> status, which is where that path is visible.
/// </para>
/// <para>
/// <b>No tenant dimension, and no sentinel enrolment.</b> A heap ceiling is a
/// property of the host process and belongs to no tenant's traffic, so as with
/// <see cref="RepoContextHeapCeilingMeter"/> these carry no tenant tag. The
/// <c>PlatformSentinelInstruments</c> array in <c>TenantMetricDimensionHygieneTests</c>
/// does not apply either, and for two independent reasons - worth stating separately,
/// because each alone would be enough and neither is load-bearing on the other:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Scope.</b> Every test consuming that array is driven by
/// <c>MetricEmissionScanner.Scan</c>, whose only directory walk enumerates
/// <c>Path.Combine(repoRoot, "src")</c>. <c>apps/</c> is never enumerated.
/// </description></item>
/// <item><description>
/// <b>Shape.</b> That scanner recognises emission <i>sites</i> - <c>.Add(</c> /
/// <c>.Record(</c> calls and <c>new Measurement&lt;T&gt;(</c> constructions. Every
/// instrument here is <i>observable</i>, so it has a callback and no emission site
/// at all, and would be invisible to the gate even if this file sat under
/// <c>src/</c>. This applies to <see cref="ExhaustionEventsCounterName"/> as much as
/// to the gauges: it is an <c>ObservableCounter</c>, not a <c>Counter</c>, so it has
/// no <c>.Add(</c> either.
/// </description></item>
/// </list>
/// <para>
/// So there is no enrolment row to add, and adding one would claim coverage the scan
/// does not perform. <b>But the exemption is observed, not guaranteed.</b> The
/// scanner fails loudly if its own <c>src/</c> walk finds nothing, so its scope
/// cannot break silently - and nothing anywhere asserts that <c>apps/</c> instruments
/// are exempt <i>on purpose</i> rather than by accident. Consequently the repository's
/// <c>Meter</c>-field-above-every-instrument rule and its observable-declared-below-
/// every-static rule are unenforced in this file and are honoured here by hand.
/// </para>
/// </remarks>
public sealed class RepoContextMemoryWatchMeter : IDisposable
{
    /// <summary>
    /// The highest memory commitment observed this run, in bytes: the high-water mark
    /// of <c>lattice_repocontext_heap_committed_bytes</c>.
    /// </summary>
    public const string PeakCommittedBytesGaugeName =
        "lattice_repocontext_heap_committed_peak_bytes";

    /// <summary>
    /// The share of the granted managed-heap ceiling consumed at the peak, as a
    /// fraction: the margin this deployment actually had left.
    /// </summary>
    public const string PeakOccupancyRatioGaugeName =
        "lattice_repocontext_heap_peak_occupancy_ratio";

    /// <summary>
    /// Managed <see cref="OutOfMemoryException"/> observed this run.
    /// </summary>
    public const string ExhaustionEventsCounterName =
        "lattice_repocontext_heap_exhaustion_events_total";

    /// <summary>
    /// The largest managed-heap ceiling at which this deployment has ever recorded
    /// running out of memory, in bytes. Absent when nothing is recorded.
    /// </summary>
    public const string InsufficientLimitBytesGaugeName =
        "lattice_repocontext_heap_insufficient_limit_bytes";

    // Declared above the instruments it constructs, and all of them are built from
    // this field, so reordering throws at initialisation rather than silently
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md - and note that this rule is UNENFORCED here:
    // the repository-wide gates enumerate src/ only, and they recognise emission
    // sites, which an observable instrument does not have. Both exemptions are real
    // and currently correct; neither is asserted anywhere, so this is upheld by hand.
    private readonly Meter _meter;

    private readonly Func<RepoContextMemorySample> _read;
    private readonly Func<long?> _readInsufficientLimitBytes;

    /// <summary>Creates the meter and publishes all four instruments.</summary>
    /// <param name="read">Reads the current high-water sample.</param>
    /// <param name="readInsufficientLimitBytes">
    /// Reads the recorded ever-worst exhaustion ceiling, or <see langword="null"/>
    /// when none is recorded.
    /// </param>
    /// <exception cref="ArgumentNullException">Either delegate is null.</exception>
    public RepoContextMemoryWatchMeter(
        Func<RepoContextMemorySample> read,
        Func<long?> readInsufficientLimitBytes)
    {
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(readInsufficientLimitBytes);

        _read = read;
        _readInsufficientLimitBytes = readInsufficientLimitBytes;

        _meter = new Meter(RepoContextHostMeter.Name);
        _meter.CreateObservableGauge(
            PeakCommittedBytesGaugeName,
            () => (double)_read().PeakCommittedBytes,
            unit: "By",
            description:
                "Highest memory commitment observed this run: the high-water mark of "
                + RepoContextHeapCeilingMeter.CommittedBytesGaugeName
                + ". This is what the corpus on this host actually required, as opposed to what a "
                + "deploy-time model predicted it would. Sampled periodically, so a spike falling "
                + "entirely between two samples is missed and this is a floor on the true peak, never "
                + "an over-statement. Zero until the first sample completes.");
        _meter.CreateObservableGauge(
            PeakOccupancyRatioGaugeName,
            () =>
            {
                // One reading for both sides, so the numerator and denominator are
                // provably the same observation rather than two scrapes divided.
                var sample = _read();
                return sample.PeakOccupancyRatio ?? 0d;
            },
            description:
                "Share of the granted managed-heap ceiling consumed at this run's peak, as a fraction of "
                + "1: the margin the deployment actually had. This is the figure to size the next grant "
                + "from, because it is normalised against whatever ceiling this process was given and so "
                + "transfers between hosts, where a byte count does not. A value approaching 1 means the "
                + "grant is being fully consumed and exhaustion is near, not that the process is "
                + "efficient. Derived at scrape time from a single reading of "
                + PeakCommittedBytesGaugeName
                + " and "
                + RepoContextHeapCeilingMeter.LimitBytesGaugeName
                + ". Zero when the runtime has reported no usable ceiling.");
        _meter.CreateObservableCounter(
            ExhaustionEventsCounterName,
            () => _read().ExhaustionEvents,
            description:
                "Managed OutOfMemoryException observed in this process this run, counted first-chance "
                + "because the documented failure is CAUGHT - it surfaces as a STORAGE error reading "
                + "grain state, so nothing terminates and no unhandled-exception path ever sees it. Any "
                + "non-zero value means the memory grant is too small for this corpus and the next start "
                + "will refuse it unless the grant is raised. A zero is evidence about managed "
                + "exhaustion ONLY: a cgroup out-of-memory kill delivers SIGKILL, raises no exception, "
                + "and is counted here as zero, so read this beside the container's restart count and "
                + "OOMKilled status rather than as proof that memory sufficed. Observable, so it is "
                + "published from process start and an absent series means this meter was not "
                + "constructed, never that no exhaustion occurred.");

        // Declared last: it is the only instrument here that may decline to report,
        // and it reads a delegate rather than a sample, so keeping it at the end
        // keeps the ordering rule above trivially satisfied as this class grows.
        _meter.CreateObservableGauge(
            InsufficientLimitBytesGaugeName,
            () =>
            {
                var recorded = _readInsufficientLimitBytes();
                return recorded is { } limit
                    ? new[] { new Measurement<double>(limit) }
                    : Array.Empty<Measurement<double>>();
            },
            unit: "By",
            description:
                "Largest managed-heap ceiling at which this deployment has ever recorded running out of "
                + "memory. A start granted this much or less is refused, so this is the number a grant "
                + "must exceed, and it is the exact value "
                + RepoContextMemoryAdmission.OverrideKey
                + " must be set to in order to override that refusal - which matters because the image "
                + "is distroless and there is no shell to read the recorded evidence with. It reports NO "
                + "measurement, rather than a zero, when nothing is recorded: an absent series here means "
                + "no exhaustion has ever been recorded against this data root, and a zero would read as "
                + "an exhaustion at a ceiling of zero bytes. Carried forward across runs, so raising the "
                + "grant does not erase the evidence that the smaller one failed.");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}
