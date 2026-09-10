using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes the process's accumulated garbage-collector pause time as a queryable
/// instrument, together with the collection count that denominates it.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2605. The runtime already emits the pause total by name, and #2600 made
/// the host report it once at startup, where it is necessarily near zero: the value
/// of that line is that it names the measurand, not that it carries a number. What
/// was missing is queryability. During the bucket-4 gate investigation two people
/// spent two rounds building a proxy for pause time out of gaps between log
/// timestamps while the quantity itself was in the same file both were reading, and
/// there was still no way to ask how much wall-clock the process spent suspended in
/// the last hour, to alert on it, or to correlate it with request latency. This host
/// runs a multi-GiB heap by design, so collector pauses are a first-class
/// explanation for a request timeout.
/// </para>
/// <para>
/// <b>Two instruments, because one of them alone cannot be read.</b> Pause seconds
/// on its own has the defect this bucket exists to close: a reading of zero is
/// indistinguishable between "the collector has run and suspended this process for
/// less than the reported resolution" and "the collector has not run at all yet".
/// Publishing the cumulative collection count beside it supplies the denominator. A
/// zero on pause seconds beside a rising collection count is a <i>measured</i>
/// absence of pause; both at zero means no collection has happened yet, which is a
/// different and much less interesting fact. Neither reading requires a log scrape
/// to disambiguate.
/// </para>
/// <para>
/// <b>Both are observable counters, deliberately.</b> The runtime exposes both
/// quantities as cumulative figures, so a counter matches their semantics exactly
/// and a scrape gap loses resolution rather than corrupting the series. A gauge
/// would misreport across a gap. Being observable, they are sampled at scrape time
/// rather than pushed, which is correct because <see cref="GC.GetTotalPauseDuration"/>
/// and <see cref="GC.CollectionCount(int)"/> are cheap reads of counters the runtime
/// already maintains.
/// </para>
/// <para>
/// <b>Absence means something different from zero, and is readable.</b> An
/// observable instrument is sampled on every scrape from the moment it is
/// published, so these series exist from process start rather than appearing on a
/// first occurrence. That is the same property the repository-context counters get
/// from pre-minting (issue #2515), and it matters for the same reason: a series
/// whose first occurrence falls after the collector reaches a series ceiling is
/// refused at creation and never appears. An <i>absent</i> series here therefore
/// means this type was never constructed or the collector refused it, never that
/// the process has not paused.
/// </para>
/// <para>
/// <b>No tenant dimension.</b> A collector pause is a property of the host process
/// and belongs to no tenant's traffic, so these series are invisible to a
/// tenant-scoped query by construction. That is a declaration rather than an
/// omission. The <c>PlatformSentinelInstruments</c> registry in
/// <c>TenantMetricDimensionHygieneTests</c> does not apply: it and
/// <c>MetricEmissionScanner</c> both enumerate <c>src/</c> only, and this type lives
/// under <c>apps/</c>, so there is no entry to add and adding one would claim
/// coverage the scan does not perform.
/// </para>
/// <para>
/// The meter is the host meter, so no collector change is needed: the exposition
/// subscribes by meter-name prefix and compares case-insensitively, and
/// <c>orleans.lattice.repocontext.host</c> already matches.
/// </para>
/// </remarks>
public sealed class RepoContextGarbageCollectionMeter : IDisposable
{
    /// <summary>
    /// Accumulated seconds this process has spent suspended for garbage collection
    /// since it started.
    /// </summary>
    public const string PauseSecondsCounterName = "lattice_repocontext_gc_pause_seconds_total";

    /// <summary>
    /// Garbage collections this process has completed since it started, across every
    /// generation. This is the denominator that makes a zero on
    /// <see cref="PauseSecondsCounterName"/> readable.
    /// </summary>
    public const string CollectionsCounterName = "lattice_repocontext_gc_collections_total";

    // Declared above the instruments it constructs, and both are built from this
    // field, so reordering throws at initialisation rather than publishing an
    // instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;

    private readonly Func<TimeSpan> _readPauseTotal;
    private readonly Func<long> _readCollectionCount;

    /// <summary>
    /// Creates the meter and publishes both instruments.
    /// </summary>
    /// <param name="readPauseTotal">
    /// Reads the cumulative pause total. Defaults to
    /// <see cref="GC.GetTotalPauseDuration"/>; a test substitutes it.
    /// </param>
    /// <param name="readCollectionCount">
    /// Reads the cumulative collection count across all generations. Defaults to the
    /// sum of <see cref="GC.CollectionCount(int)"/> over
    /// <see cref="GC.MaxGeneration"/>; a test substitutes it.
    /// </param>
    public RepoContextGarbageCollectionMeter(
        Func<TimeSpan>? readPauseTotal = null,
        Func<long>? readCollectionCount = null)
    {
        _readPauseTotal = readPauseTotal ?? GC.GetTotalPauseDuration;
        _readCollectionCount = readCollectionCount ?? ReadTotalCollections;

        _meter = new Meter(RepoContextDrainForecastService.MeterName);
        _meter.CreateObservableCounter(
            PauseSecondsCounterName,
            () => _readPauseTotal().TotalSeconds,
            unit: "s",
            description:
                "Accumulated seconds this process has spent suspended for garbage collection since it "
                + "started, sampled at scrape time from the runtime's own cumulative figure. Read it "
                + "against "
                + CollectionsCounterName
                + ": a zero here beside a rising collection count is a measured absence of pause, whereas "
                + "both reading zero means no collection has happened yet. The series is published from "
                + "process start and sampled on every scrape, so it being absent rather than zero means "
                + "the host did not construct this meter or the collector refused the series at a "
                + "ceiling, and never that the process has not paused.");
        _meter.CreateObservableCounter(
            CollectionsCounterName,
            _readCollectionCount,
            unit: "{collection}",
            description:
                "Garbage collections this process has completed since it started, summed across every "
                + "generation and sampled at scrape time. It exists to denominate "
                + PauseSecondsCounterName
                + ", which cannot be read on its own: without it, a pause total of zero is "
                + "indistinguishable between a collector that has run without measurable pause and a "
                + "collector that has not run.");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private static long ReadTotalCollections()
    {
        long total = 0;
        for (var generation = 0; generation <= GC.MaxGeneration; generation++)
        {
            total += GC.CollectionCount(generation);
        }

        return total;
    }
}
