using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.GrainIndex.Observability;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.GrainIndex</c>. Every grain-index
/// instrument is published on the <b>shared core meter</b>
/// (<see cref="LatticeMetrics.Meter"/>, named <see cref="MeterName"/>) and
/// carries an <see cref="TagIndex"/> tag naming the index it belongs to.
/// </summary>
/// <remarks>
/// <para>
/// The package deliberately publishes no meter of its own. A deployment that
/// already subscribes an OpenTelemetry pipeline to <c>orleans.lattice</c> - which
/// every lattice deployment does - picks the index series up with no extra
/// wiring, and an index series sits alongside the tree series it is derived
/// from rather than in a separate namespace an operator has to remember to
/// enable.
/// </para>
/// <para>
/// Recording is cheap by construction. Each instrument is consulted through its
/// <c>Enabled</c> flag before any measurement work happens, so an unsubscribed
/// process pays a predictable-branch and nothing else. Tags are pre-built:
/// <see cref="IndexTag"/> hands back one cached
/// <see cref="KeyValuePair{TKey, TValue}"/> per index name (callers that are
/// per-index singletons cache it in a field), and the path tags are static
/// readonly values, so a measurement never allocates a tag array, never boxes a
/// value, and never builds a string.
/// </para>
/// <para>
/// Every grain-index series also carries the repository-wide derived
/// <see cref="LatticeTenantLabel.TagTenant"/> dimension. A grain index is a
/// cluster-local construct in the reserved
/// <see cref="GrainIndexTreeNames.ReservedPrefix"/> namespace that spans every
/// grain of its type, so a measurement is not attributable to any one tenant and
/// always names the constant platform sentinel
/// (<see cref="LatticeTenantLabel.Platform"/>). The dimension is present on
/// tenancy-on and tenancy-off clusters alike, so an index telemetry query is
/// byte-identical in both deployment modes. Because the sentinel is a single
/// frozen singleton, adding it costs no per-measurement allocation.
/// </para>
/// <para>
/// The observable gauges read a frozen snapshot published by the backfill grain
/// (see <c>GrainIndexBackfillProgressRegistry</c>), so a scrape recomputes
/// nothing and touches no durable store. They report for the silo hosting a
/// crawl's activation, which is the silo that knows where the crawl has reached.
/// </para>
/// </remarks>
public static class GrainIndexMetrics
{
    /// <summary>
    /// The meter every grain-index instrument is published on: the shared core
    /// lattice meter, not a package-specific one.
    /// </summary>
    public const string MeterName = LatticeMetrics.MeterName;

    /// <summary>
    /// The tag key carrying the logical index name on every grain-index series.
    /// The single dimension by which index telemetry is attributable.
    /// </summary>
    public const string TagIndex = "index";

    /// <summary>
    /// The tag key naming the onboarding route a measurement belongs to. Its
    /// values are <see cref="PathActivation"/>, <see cref="PathBackfill"/>, and
    /// <see cref="PathOutbox"/>.
    /// </summary>
    public const string TagPath = "path";

    /// <summary>
    /// The <see cref="TagPath"/> value for work done by a grain's own
    /// activation or mutation path - the route that physically writes a grain's
    /// entries.
    /// </summary>
    public const string PathActivation = "activation";

    /// <summary>
    /// The <see cref="TagPath"/> value for work driven by the background
    /// backfill crawl.
    /// </summary>
    public const string PathBackfill = "backfill";

    /// <summary>
    /// The <see cref="TagPath"/> value for work done by the pending-projection
    /// outbox drain - a deferred or retried index write.
    /// </summary>
    public const string PathOutbox = "outbox";

    /// <summary>
    /// Canonical name of the enrolled-grains counter: one measurement per grain
    /// a route onboards into an index, tagged by <see cref="TagIndex"/> and
    /// <see cref="TagPath"/>.
    /// </summary>
    /// <remarks>
    /// The two series answer different questions and are deliberately not
    /// additive. <see cref="PathActivation"/> counts the enrolments that were
    /// <i>performed</i>, which is every enrolment, because a grain is only ever
    /// indexed by its own activation. <see cref="PathBackfill"/> counts the ones
    /// the crawl <i>caused</i>, which is how much of a dormant population the
    /// crawl has onboarded. A crawl-driven grain therefore appears on both;
    /// chart them side by side rather than summing them.
    /// </remarks>
    public const string GrainsEnrolledName = "orleans.lattice.grainindex.grains_enrolled";

    /// <summary>
    /// Canonical name of the index entry-count up-down counter: the net change
    /// in the number of entries an index holds, tagged by
    /// <see cref="TagIndex"/>. Summed over time it is the index's live entry
    /// count.
    /// </summary>
    /// <remarks>
    /// Fed from <see cref="GrainIndexUpdatePlan.EntryDelta"/>, which the diff
    /// computes exactly, so an in-place payload rewrite (an unordered property
    /// whose key does not move) correctly contributes zero rather than looking
    /// like a new entry.
    /// </remarks>
    public const string EntriesName = "orleans.lattice.grainindex.entries";

    /// <summary>
    /// Canonical name of the index-write-failure counter: one measurement each
    /// time a route fails to publish a grain's entries, tagged by
    /// <see cref="TagIndex"/> and <see cref="TagPath"/>.
    /// </summary>
    public const string WriteFailuresName = "orleans.lattice.grainindex.write_failures";

    /// <summary>
    /// Canonical name of the projection-latency histogram, in milliseconds: the
    /// time to project one grain's state into entries and reconcile them
    /// against the projection the index already holds. Tagged by
    /// <see cref="TagIndex"/>.
    /// </summary>
    public const string ProjectionDurationName = "orleans.lattice.grainindex.projection.duration";

    /// <summary>
    /// Canonical name of the backfill processed-keys observable gauge: how many
    /// keys the crawl has taken from its key source so far. Tagged by
    /// <see cref="TagIndex"/>. Always reported for an index whose crawl this
    /// silo hosts, whether or not a total is known.
    /// </summary>
    public const string BackfillProcessedName = "orleans.lattice.grainindex.backfill.processed";

    /// <summary>
    /// Canonical name of the backfill total-keys observable gauge: the
    /// best-effort size of the population the crawl has to cover, as reported by
    /// the application's key source. Tagged by <see cref="TagIndex"/>. A key
    /// source that cannot bound its population contributes no measurement at
    /// all, rather than a misleading zero.
    /// </summary>
    public const string BackfillTotalName = "orleans.lattice.grainindex.backfill.total";

    /// <summary>
    /// Canonical name of the backfill percent-complete observable gauge (0 to
    /// 100), tagged by <see cref="TagIndex"/>. Reported when the key source can
    /// bound the population, and for a completed crawl (which is complete by
    /// definition); otherwise no measurement is emitted and
    /// <see cref="BackfillProcessedName"/> is the progress signal.
    /// </summary>
    public const string BackfillPercentCompleteName = "orleans.lattice.grainindex.backfill.percent_complete";

    /// <summary>
    /// Canonical name of the backfill lifecycle-state observable gauge, tagged
    /// by <see cref="TagIndex"/>. The value is the numeric
    /// <see cref="GrainIndexBackfillState"/>: <c>0</c> not started, <c>1</c>
    /// running, <c>2</c> paused, <c>3</c> completed, <c>4</c> failed.
    /// </summary>
    public const string BackfillStateName = "orleans.lattice.grainindex.backfill.state";

    /// <summary>
    /// The meter every grain-index instrument is published on. It is the core
    /// lattice meter itself, exposed here so a test or a custom exporter can
    /// subscribe by reference without also referencing the core metrics type.
    /// </summary>
    public static readonly Meter Meter = LatticeMetrics.Meter;

    /// <summary>
    /// Counts grains onboarded into an index, tagged by index and by the route
    /// that onboarded them. See <see cref="GrainsEnrolledName"/> for how the two
    /// route series relate.
    /// </summary>
    public static readonly Counter<long> GrainsEnrolled = Meter.CreateCounter<long>(
        GrainsEnrolledName,
        "{grain}",
        "Grains onboarded into a grain index, by the route that onboarded them.");

    /// <summary>
    /// Tracks the net change in an index's entry count, so the running sum is
    /// the number of entries the index currently holds.
    /// </summary>
    public static readonly UpDownCounter<long> Entries = Meter.CreateUpDownCounter<long>(
        EntriesName,
        "{entry}",
        "Net change in the number of entries a grain index holds.");

    /// <summary>
    /// Counts failures to publish a grain's index entries, tagged by index and
    /// by the route that failed.
    /// </summary>
    public static readonly Counter<long> WriteFailures = Meter.CreateCounter<long>(
        WriteFailuresName,
        "{failure}",
        "Failures to publish a grain's index entries, by the route that failed.");

    /// <summary>
    /// Records how long it took to project one grain's state and reconcile it
    /// against the index's current projection for that grain.
    /// </summary>
    public static readonly Histogram<double> ProjectionDuration = Meter.CreateHistogram<double>(
        ProjectionDurationName,
        "ms",
        "Time to project one grain's state into index entries and diff it against the stored projection.");

    /// <summary>
    /// Reports how many keys each hosted backfill crawl has taken from its key
    /// source.
    /// </summary>
    public static readonly ObservableGauge<long> BackfillProcessed = Meter.CreateObservableGauge(
        BackfillProcessedName,
        GrainIndexBackfillProgressRegistry.ObserveProcessed,
        "{grain}",
        "Keys a grain index's background backfill has taken from its key source.");

    /// <summary>
    /// Reports the best-effort population size each hosted backfill crawl has to
    /// cover, for the indexes whose key source can bound it.
    /// </summary>
    public static readonly ObservableGauge<long> BackfillTotal = Meter.CreateObservableGauge(
        BackfillTotalName,
        GrainIndexBackfillProgressRegistry.ObserveTotal,
        "{grain}",
        "Best-effort size of the population a grain index's background backfill has to cover.");

    /// <summary>
    /// Reports how far through its population each hosted backfill crawl is, as
    /// a percentage, for the indexes where that is knowable.
    /// </summary>
    public static readonly ObservableGauge<double> BackfillPercentComplete = Meter.CreateObservableGauge(
        BackfillPercentCompleteName,
        GrainIndexBackfillProgressRegistry.ObservePercentComplete,
        "%",
        "How far through its population a grain index's background backfill has reached.");

    /// <summary>
    /// Reports the lifecycle state of each hosted backfill crawl as the numeric
    /// <see cref="GrainIndexBackfillState"/>.
    /// </summary>
    public static readonly ObservableGauge<int> BackfillState = Meter.CreateObservableGauge(
        BackfillStateName,
        GrainIndexBackfillProgressRegistry.ObserveState,
        "{state}",
        "Lifecycle state of a grain index's background backfill, as the numeric GrainIndexBackfillState.");

    /// <summary>
    /// The pre-built <see cref="TagPath"/> tag for the activation and mutation
    /// route.
    /// </summary>
    internal static readonly KeyValuePair<string, object?> ActivationPathTag = new(TagPath, PathActivation);

    /// <summary>The pre-built <see cref="TagPath"/> tag for the backfill crawl.</summary>
    internal static readonly KeyValuePair<string, object?> BackfillPathTag = new(TagPath, PathBackfill);

    /// <summary>The pre-built <see cref="TagPath"/> tag for the outbox drain.</summary>
    internal static readonly KeyValuePair<string, object?> OutboxPathTag = new(TagPath, PathOutbox);

    private static readonly ConcurrentDictionary<string, KeyValuePair<string, object?>> IndexTags =
        new(StringComparer.Ordinal);

    private static readonly Func<string, KeyValuePair<string, object?>> BuildIndexTag =
        static name => new KeyValuePair<string, object?>(TagIndex, name);

    /// <summary>
    /// The cached <see cref="TagIndex"/> tag for <paramref name="indexName"/>.
    /// </summary>
    /// <remarks>
    /// The value is interned per index name so a recording site can pass the tag
    /// straight to an instrument without allocating one per measurement. A
    /// caller that is itself a per-index singleton should cache the result in a
    /// field and skip even this lookup.
    /// </remarks>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The tag naming that index.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public static KeyValuePair<string, object?> IndexTag(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return IndexTags.GetOrAdd(indexName, BuildIndexTag);
    }

    /// <summary>
    /// Records that a route onboarded <paramref name="count"/> grains into an
    /// index.
    /// </summary>
    /// <param name="indexTag">The index tag, from <see cref="IndexTag"/>.</param>
    /// <param name="pathTag">The route tag.</param>
    /// <param name="count">How many grains were onboarded. Ignored when not positive.</param>
    internal static void RecordGrainsEnrolled(
        in KeyValuePair<string, object?> indexTag,
        in KeyValuePair<string, object?> pathTag,
        long count)
    {
        if (count > 0 && GrainsEnrolled.Enabled)
            GrainsEnrolled.Add(count, indexTag, pathTag, LatticeTenantLabel.Platform);
    }

    /// <summary>Records an index's net entry-count change.</summary>
    /// <param name="indexTag">The index tag, from <see cref="IndexTag"/>.</param>
    /// <param name="delta">The net change. Ignored when zero.</param>
    internal static void RecordEntryDelta(in KeyValuePair<string, object?> indexTag, int delta)
    {
        if (delta != 0 && Entries.Enabled)
            Entries.Add(delta, indexTag, LatticeTenantLabel.Platform);
    }

    /// <summary>Records failures to publish a grain's index entries.</summary>
    /// <param name="indexTag">The index tag, from <see cref="IndexTag"/>.</param>
    /// <param name="pathTag">The route tag.</param>
    /// <param name="count">How many failures. Ignored when not positive.</param>
    internal static void RecordWriteFailures(
        in KeyValuePair<string, object?> indexTag,
        in KeyValuePair<string, object?> pathTag,
        long count)
    {
        if (count > 0 && WriteFailures.Enabled)
            WriteFailures.Add(count, indexTag, pathTag, LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records a projection's elapsed time from a
    /// <see cref="System.Diagnostics.Stopwatch"/> timestamp taken before it
    /// began.
    /// </summary>
    /// <param name="indexTag">The index tag, from <see cref="IndexTag"/>.</param>
    /// <param name="startTimestamp">The timestamp taken before the projection.</param>
    internal static void RecordProjectionDuration(
        in KeyValuePair<string, object?> indexTag,
        long startTimestamp)
    {
        if (ProjectionDuration.Enabled)
        {
            ProjectionDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
                indexTag,
                LatticeTenantLabel.Platform);
        }
    }
}
