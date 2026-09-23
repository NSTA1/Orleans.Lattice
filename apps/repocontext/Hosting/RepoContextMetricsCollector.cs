using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Subscribes to every Lattice-owned <see cref="Meter"/> in the process and
/// accumulates its measurements so the container can serve them from a
/// Prometheus scrape endpoint (<see cref="RepoContextHostBuilder.MetricsPath"/>).
/// </summary>
/// <remarks>
/// <para>
/// This exists because the container's instruments were unreadable in the
/// deployment that needs them (issue #2363): the image is distroless, so there is
/// no shell to inspect from, and the host mapped no scrape endpoint. An
/// instrument nobody can scrape is not yet an instrument.
/// </para>
/// <para>
/// It is deliberately dependency-free. Pulling an OpenTelemetry Prometheus
/// exporter into the image would add a pre-release package to an otherwise
/// release-pinned application for a surface this small, so the collector is a
/// plain <see cref="MeterListener"/> over the instruments the process already
/// publishes.
/// </para>
/// <para>
/// <b>Instrument selection is by meter NAME, never by reference.</b> Selecting
/// with <c>ReferenceEquals(instrument.Meter, SomeMetrics.Meter)</c> would run that
/// class's static initialiser re-entrantly from inside
/// <see cref="MeterListener.InstrumentPublished"/>, which is exactly the silent
/// failure the repository's meter-field declaration-order convention exists to
/// prevent. A name comparison touches no other type's statics, so this collector
/// cannot trigger it.
/// </para>
/// </remarks>
public sealed class RepoContextMetricsCollector : IDisposable
{
    /// <summary>
    /// The case-insensitive meter-name prefix covering the Lattice estate. Every
    /// Lattice meter is named under it - the core <c>orleans.lattice</c> meter and
    /// its per-package siblings, and the repository-context surface's own
    /// <c>Orleans.Lattice.Api.Mcp.RepoContext</c> meter - so one prefix covers the
    /// whole estate and a meter added by a future package is picked up without a
    /// code change here. It is one of <see cref="SubscribedMeterNamePrefixes"/>, not
    /// the only one: the runtime families below are deliberately outside it.
    /// </summary>
    public const string MeterNamePrefix = "orleans.lattice";

    /// <summary>
    /// The BCL's built-in runtime meter. It is in the base class library, needs no
    /// package reference and no opt-in beyond a listener, and carries the only
    /// in-process account of what this container's memory is doing:
    /// <c>dotnet.process.memory.working_set</c>, <c>dotnet.gc.last_collection.heap.size</c>,
    /// <c>dotnet.gc.last_collection.memory.committed_size</c>,
    /// <c>dotnet.gc.heap.total_allocated</c>, and <c>dotnet.gc.collections</c>
    /// partitioned by generation.
    /// </summary>
    /// <remarks>
    /// Issue #2543. This meter is not under <see cref="MeterNamePrefix"/>, so before
    /// this constant existed the collector recorded every Lattice instrument and
    /// discarded the entire runtime family at the listener. The consequence was not
    /// a missing nicety: a container running against a hard memory limit exported no
    /// instrument that could attribute a single byte of it, so every memory question
    /// this reliability epic asked was answered by inference or by <c>docker stats</c>
    /// from outside, and heap-ceiling adherence - the claimed effect of two merged
    /// fixes in this same epic - was not measurable from inside at all.
    /// <para>
    /// Adding it costs no dependency, which is what keeps the class remarks above
    /// true: this is still a plain <see cref="MeterListener"/> and the image still
    /// gains no OpenTelemetry package.
    /// </para>
    /// </remarks>
    public const string RuntimeMeterNamePrefix = "System.Runtime";

    /// <summary>
    /// Orleans' own runtime meter: grain and system-target activation counts,
    /// activation latency, messaging, directory, and scheduler instruments.
    /// </summary>
    /// <remarks>
    /// Issue #2724. The name is the one verified against the pinned Orleans version
    /// by the reference-architecture registration (PR #2750) rather than recalled,
    /// because the failure mode of getting it wrong is silent in exactly the way this
    /// subscription exists to remove: a name that matches nothing registers nothing,
    /// throws nothing, and leaves an endpoint byte-identical to the one that has the
    /// bug.
    /// <para>
    /// PR #2750 fixed the two reference-architecture silos, which register with
    /// OpenTelemetry. This container does not: it is distroless and dependency-free
    /// and collects through this listener instead, so it did not inherit that fix and
    /// the live rig still discarded every <c>Microsoft.Orleans</c> series. Activation
    /// latency is the most direct signal for the inline-capture risk issue #2696
    /// describes, and it was being recorded and thrown away one layer above the
    /// instrument.
    /// </para>
    /// </remarks>
    public const string OrleansRuntimeMeterNamePrefix = "Microsoft.Orleans";

    /// <summary>
    /// Every meter-name prefix this collector subscribes to, compared
    /// case-insensitively. Ordered most-specific-first is unnecessary because the
    /// prefixes are disjoint; the order here is the order the subscription-proof
    /// series renders in.
    /// </summary>
    public static readonly IReadOnlyList<string> SubscribedMeterNamePrefixes =
    [
        MeterNamePrefix,
        OrleansRuntimeMeterNamePrefix,
        RuntimeMeterNamePrefix,
    ];

    /// <summary>
    /// The gauge naming each meter-name prefix this collector subscribes to, valued
    /// with the number of distinct meters it has matched.
    /// </summary>
    /// <remarks>
    /// This exists because a series discarded at the listener is otherwise
    /// indistinguishable from one that was never declared. Issue #2724 sets that out
    /// as a three-way table in which two of the rows are discriminable and the third
    /// is not: an instrument that is declared, firing, and dropped at the listener is
    /// byte-identical on the endpoint to one that does not exist, and byte-identical
    /// in the source to a healthy one. It is invisible to an endpoint scan and to
    /// source inspection alike, which is how a survey of this very rig concluded that
    /// Orleans exports no activation-duration instrument when the truth was that
    /// Orleans exports it and the host threw it away.
    /// <para>
    /// One series per configured prefix, minted at zero when the collector is
    /// constructed and therefore before any instrument can have been published,
    /// resolves the table. An <b>absent</b> prefix means this build does not
    /// subscribe to that family - which is what a pre-fix image looks like. A prefix
    /// present at <b>zero</b> means the subscription landed and nothing has published
    /// on that meter, which is a statement about the process rather than about the
    /// build. A non-zero value names how many meters were matched. The zero is a
    /// reachability proof for the subscription itself, in the sense the repository's
    /// zero-priming convention intends.
    /// </para>
    /// <para>
    /// Like the drop attribution, these samples are rendered straight from the
    /// collector's own state rather than routed through the family and series
    /// machinery, so a ceiling can never suppress the diagnostic that says which
    /// families are being collected. Cardinality is fixed by
    /// <see cref="SubscribedMeterNamePrefixes"/>, which is fixed by code.
    /// </para>
    /// </remarks>
    public const string SubscribedMetersGaugeName = "lattice_metrics_subscribed_meters";

    /// <summary>The label naming the subscribed prefix on <see cref="SubscribedMetersGaugeName"/>.</summary>
    public const string PrefixLabelName = "prefix";

    /// <summary>
    /// The default ceiling on distinct series within a single metric family. A tag
    /// value the collector did not anticipate could otherwise grow the exposition
    /// without bound, so the cap fails closed: measurements beyond it are dropped
    /// and counted rather than retained.
    /// </summary>
    /// <remarks>
    /// The ceiling is deliberately PER FAMILY rather than global. The hazard it
    /// guards against - one instrument acquiring an unanticipated high-cardinality
    /// tag such as a repository id, a path, or a key - belongs to that instrument,
    /// and a global ceiling lets the offending family consume the entire budget and
    /// then permanently block every OTHER family from ever creating a series.
    /// <para>
    /// That failure is silent, which is what makes it worth this note. A series
    /// that already exists keeps updating, because the lookup precedes the ceiling
    /// check, so a saturated exposition still looks busy and complete. Only a
    /// series whose FIRST occurrence falls after saturation is missing, and it is
    /// missing permanently. Issue #2480 is exactly that: the ANN search counter's
    /// <c>bootstrapping</c> and <c>exhaustive</c> arms are created within seconds of
    /// start-up and publish forever, while its <c>approximate</c> arm cannot occur
    /// until a plane has trained - hours later, past saturation - so a trained plane
    /// was unobservable and indistinguishable from one that never armed.
    /// </para>
    /// </remarks>
    public const int DefaultMaxSeriesPerFamily = 10_000;

    /// <summary>
    /// The default ceiling on distinct exposed series across every family. This is
    /// a memory backstop, not the cardinality control: families are created only
    /// from published instruments, so their number is fixed by code and cannot grow
    /// from tag cardinality. It is set far above any healthy estate deliberately,
    /// because a global ceiling that binds in normal operation reintroduces the
    /// cross-family starvation that <see cref="DefaultMaxSeriesPerFamily"/> exists
    /// to prevent. Reaching it means the process is misconfigured.
    /// </summary>
    public const int DefaultMaxSeries = 250_000;

    /// <summary>The gauge reporting how many series the collector currently holds.</summary>
    public const string SeriesGaugeName = "lattice_metrics_series";

    /// <summary>The counter reporting measurements dropped because a series ceiling was reached.</summary>
    public const string DroppedCounterName = "lattice_metrics_dropped_measurements_total";

    /// <summary>
    /// The counter attributing dropped measurements to the family that was refused
    /// and the ceiling that refused it (<c>family</c> or <c>global</c>).
    /// </summary>
    /// <remarks>
    /// A ceiling that drops silently is the same defect class as the one the
    /// per-family ceiling exists to fix: it makes an absent series ambiguous between
    /// "never recorded" and "recorded and refused", which is precisely the ambiguity
    /// that left issue #2480 undiagnosed. Attribution resolves it directly, and
    /// naming the ceiling separates a single exploding instrument from an estate
    /// that has reached the memory backstop.
    /// <para>
    /// These samples are rendered straight from the collector's own state rather
    /// than routed through the family and series machinery, so the diagnostic can
    /// never be suppressed by the ceilings it reports on. Its cardinality is bounded
    /// by the number of published instruments, which is fixed by code.
    /// </para>
    /// </remarks>
    public const string DroppedByFamilyCounterName = "lattice_metrics_dropped_measurements_by_family_total";

    /// <summary>The label naming the refused family on <see cref="DroppedByFamilyCounterName"/>.</summary>
    public const string FamilyLabelName = "family";

    /// <summary>The label naming the ceiling that refused a measurement.</summary>
    public const string CeilingLabelName = "ceiling";

    /// <summary>The <see cref="CeilingLabelName"/> value for the per-family ceiling.</summary>
    public const string FamilyCeilingLabel = "family";

    /// <summary>The <see cref="CeilingLabelName"/> value for the global backstop.</summary>
    public const string GlobalCeilingLabel = "global";

    /// <summary>
    /// The pending-text threshold at which <see cref="WriteToAsync"/> flushes to the
    /// response. It bounds the peak held body: the streaming path never holds more than
    /// this plus whatever the family being rendered added past it. Small enough that the
    /// builder's chunks stay well under the 85 KB large-object-heap threshold, which is
    /// the whole point of issue #3136.
    /// </summary>
    private const int FlushThresholdChars = 16 * 1024;

    /// <summary>
    /// The pooled UTF-8 destination buffer size. It is deliberately independent of
    /// <see cref="FlushThresholdChars"/>: the flush converts in bounded slices, so this
    /// buffer never has to be large enough for the worst-case expansion of the pending
    /// text, and sizing it this way keeps it off the large object heap.
    /// </summary>
    private const int FlushBufferBytes = 8 * 1024;

    private readonly ConcurrentDictionary<string, MetricFamily> _families = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Family, string Ceiling), DropCount> _dropsByFamily = new();

    /// <summary>
    /// The distinct meter names matched under each subscribed prefix. Every prefix
    /// gets an entry at construction, so the gauge it renders is minted at zero
    /// before any instrument can publish. See <see cref="SubscribedMetersGaugeName"/>.
    /// </summary>
    private readonly Dictionary<string, HashSet<string>> _matchedMetersByPrefix;

    /// <summary>
    /// The event id of the one warning emitted the first time a ceiling refuses a
    /// series. See <see cref="AttachLogger"/>.
    /// </summary>
    public static readonly EventId CeilingReachedEvent = new(1, "MetricsCeilingReached");

    private static readonly Action<ILogger, string, int, DateTimeOffset, string, long, long, Exception?> LogFamilyCeilingReached =
        LoggerMessage.Define<string, int, DateTimeOffset, string, long, long>(
            LogLevel.Warning,
            CeilingReachedEvent,
            "RepoContext metrics collector reached its {Ceiling} ceiling of {Limit} series at {SaturatedAtUtc:O} "
            + "while admitting family {Family} ({FamilySeries} series in that family, {TotalSeries} in total). "
            + "Further first-seen label sets IN THIS FAMILY will be refused for the life of this process and counted on "
            + DroppedByFamilyCounterName + "; from this instant an absent series in this family is ambiguous "
            + "between never-fired and refused.");

    private static readonly Action<ILogger, string, int, DateTimeOffset, string, long, long, Exception?> LogGlobalCeilingReached =
        LoggerMessage.Define<string, int, DateTimeOffset, string, long, long>(
            LogLevel.Warning,
            CeilingReachedEvent,
            "RepoContext metrics collector reached its {Ceiling} ceiling of {Limit} series at {SaturatedAtUtc:O} "
            + "while admitting family {Family} ({FamilySeries} series in that family, {TotalSeries} in total). "
            + "Further first-seen label sets IN EVERY FAMILY will be refused for the life of this process and counted on "
            + DroppedByFamilyCounterName + "; from this instant an absent series anywhere in this exposition is "
            + "ambiguous between never-fired and refused.");

    private readonly MeterListener _listener = new();
    private readonly int _maxSeriesPerFamily;
    private readonly int _maxSeries;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Ceiling crossings captured at the instant they happened and not yet written to
    /// a logger. A crossing can precede the logger - the collector is built before
    /// the host's container, so before any logger exists - and a transition has to be
    /// captured when it occurs, because no reading taken afterwards can recover the
    /// instant. Bounded by the number of families plus one, since each family and
    /// the global backstop are captured at most once.
    /// </summary>
    private readonly ConcurrentQueue<CeilingCrossing> _pendingCrossings = new();

    private ILogger? _logger;
    private long _seriesCount;
    private long _dropped;
    private int _globalCeilingReached;
    private int _disposed;

    /// <summary>
    /// Creates a collector and starts listening. Instruments already published by
    /// the process are replayed by <see cref="MeterListener.Start"/>, so
    /// construction order relative to the metrics classes does not matter.
    /// </summary>
    /// <param name="maxSeriesPerFamily">
    /// The ceiling on distinct series within one family; defaults to
    /// <see cref="DefaultMaxSeriesPerFamily"/>. Must be positive.
    /// </param>
    /// <param name="maxSeries">
    /// The backstop ceiling on distinct series across every family; defaults to
    /// <see cref="DefaultMaxSeries"/>. Must be positive.
    /// </param>
    /// <param name="timeProvider">
    /// The clock that stamps the instant a ceiling is first reached; defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxSeriesPerFamily"/> or <paramref name="maxSeries"/> is not positive.
    /// </exception>
    public RepoContextMetricsCollector(
        int maxSeriesPerFamily = DefaultMaxSeriesPerFamily,
        int maxSeries = DefaultMaxSeries,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSeriesPerFamily);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSeries);
        _maxSeriesPerFamily = maxSeriesPerFamily;
        _maxSeries = maxSeries;
        _timeProvider = timeProvider ?? TimeProvider.System;

        // Minted here, before Start() replays anything, so every configured prefix
        // has a series from the first scrape whether or not a meter ever publishes
        // under it. That is what makes an ABSENT prefix mean "this build does not
        // subscribe to that family" rather than "nothing happened".
        _matchedMetersByPrefix = SubscribedMeterNamePrefixes.ToDictionary(
            prefix => prefix,
            _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase),
            StringComparer.Ordinal);

        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<byte>(OnMeasurement);
        _listener.SetMeasurementEventCallback<short>(OnMeasurement);
        _listener.SetMeasurementEventCallback<int>(OnMeasurement);
        _listener.SetMeasurementEventCallback<long>(OnMeasurement);
        _listener.SetMeasurementEventCallback<float>(OnMeasurement);
        _listener.SetMeasurementEventCallback<double>(OnMeasurement);
        _listener.SetMeasurementEventCallback<decimal>(OnMeasurement);
        _listener.Start();
    }

    /// <summary>
    /// Supplies the logger that announces the instant each series ceiling is first
    /// reached, and immediately writes any crossing that happened before it was
    /// attached.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A ceiling refusal is permanent for the life of the process, and from the first
    /// one the exposition stops being a complete account of what the host emits: an
    /// absent series becomes ambiguous between "never fired" and "fired and refused".
    /// The drop counters report that as a level, which cannot say when it began, so
    /// the transition itself is announced: exactly one warning
    /// (<see cref="CeilingReachedEvent"/>) the first time the per-family ceiling
    /// refuses a series in each family, and exactly one the first time the global
    /// backstop refuses any series (issue #2519). Never one per refusal - the
    /// counters carry the ongoing volume, and per-refusal logging on a saturated host
    /// would be its own incident.
    /// </para>
    /// <para>
    /// This is a method rather than a constructor argument because the host builds
    /// the collector before its service container exists, so that startup
    /// measurements are counted, and no logger exists yet at that point. A crossing
    /// that happens before a logger is attached is captured, with its timestamp, at
    /// the instant it happens and written here, so a deferred record still states the
    /// true instant rather than the moment of attachment. Attaching again replaces
    /// the logger; a crossing already written is never written a second time.
    /// </para>
    /// </remarks>
    /// <param name="logger">The logger to announce ceiling crossings to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> is <see langword="null"/>.</exception>
    public void AttachLogger(ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        Volatile.Write(ref _logger, logger);
        AnnouncePendingCrossings();
    }

    /// <summary>
    /// Whether the supplied meter name is one this collector subscribes to.
    /// </summary>
    /// <param name="meterName">The meter name to test.</param>
    /// <returns>
    /// <see langword="true"/> when the meter is Lattice-owned, Orleans' own runtime
    /// meter, or the BCL runtime meter.
    /// </returns>
    public static bool IsSubscribedMeter(string? meterName)
        => MatchPrefix(meterName) is not null;

    /// <summary>
    /// The subscribed prefix that matches the supplied meter name, or
    /// <see langword="null"/> when none does.
    /// </summary>
    /// <param name="meterName">The meter name to classify.</param>
    /// <returns>The matching entry of <see cref="SubscribedMeterNamePrefixes"/>, or null.</returns>
    public static string? MatchPrefix(string? meterName)
    {
        if (meterName is null)
        {
            return null;
        }

        foreach (var prefix in SubscribedMeterNamePrefixes)
        {
            if (meterName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return prefix;
            }
        }

        return null;
    }

    /// <summary>
    /// Classifies a .NET instrument into the Prometheus family it renders as.
    /// </summary>
    /// <param name="instrument">The published instrument.</param>
    /// <returns>The Prometheus family.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="instrument"/> is null.</exception>
    public static RepoContextMetricKind KindOf(Instrument instrument)
    {
        ArgumentNullException.ThrowIfNull(instrument);

        var type = instrument.GetType();
        if (!type.IsGenericType)
        {
            return RepoContextMetricKind.Gauge;
        }

        var definition = type.GetGenericTypeDefinition();
        if (definition == typeof(Counter<>) || definition == typeof(ObservableCounter<>))
        {
            return RepoContextMetricKind.Counter;
        }

        if (definition == typeof(Histogram<>))
        {
            return RepoContextMetricKind.Summary;
        }

        return RepoContextMetricKind.Gauge;
    }

    /// <summary>
    /// Renders the current state as a Prometheus text exposition payload. Polls
    /// every observable instrument first, so a gauge reports the value at scrape
    /// time rather than the value at its last callback.
    /// </summary>
    /// <returns>The exposition body.</returns>
    /// <remarks>
    /// This overload materialises the whole exposition as one <see cref="string"/> and
    /// is retained for tests and for callers that genuinely want the body in hand. The
    /// scrape endpoint must not use it: see <see cref="WriteToAsync"/> for why.
    /// </remarks>
    public string Render()
    {
        PollObservableInstruments();

        var builder = new StringBuilder(4096);
        foreach (var family in OrderedFamilies())
        {
            family.Render(builder);
        }

        AppendTrailer(builder);

        return builder.ToString();
    }

    /// <summary>
    /// Writes the current state as a Prometheus text exposition payload directly to
    /// <paramref name="destination"/> as UTF-8, without ever holding the whole body in
    /// memory. Polls every observable instrument first, so a gauge reports the value at
    /// scrape time rather than the value at its last callback. The bytes written are
    /// byte-for-byte identical to <c>Encoding.UTF8.GetBytes(Render())</c>.
    /// </summary>
    /// <param name="destination">The response body to write the exposition to. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the exposition has been written.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="destination"/> is null.</exception>
    /// <remarks>
    /// <para>
    /// Issue #3136. <see cref="Render"/> ends in <c>StringBuilder.ToString()</c>, and
    /// that single call is what failed on the live container: a <see cref="StringBuilder"/>
    /// holds its content as a linked list of chunks, so building a 4.15 MB exposition
    /// never needs a contiguous block, but <c>ToString</c> must produce one
    /// <see cref="string"/> - a contiguous ~8.3 MB UTF-16 buffer, far over the 85 KB
    /// large-object-heap threshold. The LOH is not compacted by default, so a heap that
    /// is fragmented and near its ceiling can have several megabytes free in aggregate
    /// and still be unable to satisfy it. Seventeen scrapes died there with
    /// <see cref="OutOfMemoryException"/>, which is the worst possible direction to fail
    /// in: the metrics vanish under exactly the memory pressure they exist to report,
    /// and the resulting gap in the time series is indistinguishable from nothing having
    /// happened.
    /// </para>
    /// <para>
    /// This path removes that allocation rather than enlarging a buffer to survive it.
    /// Families are rendered into one small reusable builder that is flushed and cleared
    /// whenever it crosses <see cref="FlushThresholdChars"/>, so the peak held body falls
    /// from the whole exposition to roughly the threshold plus one family. The flush
    /// encodes straight to UTF-8 through a pooled byte buffer, so the separate encode
    /// buffer that <c>Results.Text</c> would have allocated goes too.
    /// </para>
    /// <para>
    /// The encoder is created once for the whole response and carries state across
    /// flushes. That is load-bearing rather than tidy: a flush boundary can fall between
    /// the two halves of a surrogate pair, and a per-flush
    /// <c>Encoding.UTF8.GetBytes</c> would emit a replacement character for each half.
    /// The exposition is ASCII in practice, but nothing in the label or help path
    /// guarantees it, so correctness here must not rest on that.
    /// </para>
    /// </remarks>
    public async Task WriteToAsync(Stream destination, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);

        PollObservableInstruments();

        var encoder = Encoding.UTF8.GetEncoder();
        var buffer = ArrayPool<byte>.Shared.Rent(FlushBufferBytes);
        try
        {
            var builder = new StringBuilder(FlushThresholdChars);

            foreach (var family in OrderedFamilies())
            {
                family.Render(builder);
                if (builder.Length >= FlushThresholdChars)
                {
                    await FlushAsync(builder, encoder, buffer, destination, false, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            AppendTrailer(builder);
            await FlushAsync(builder, encoder, buffer, destination, true, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Encodes everything currently held by <paramref name="builder"/> to UTF-8 and
    /// writes it to <paramref name="destination"/>, then clears the builder so the next
    /// families reuse it. Walks the builder's chunks rather than calling
    /// <c>ToString</c>, so no allocation proportional to the pending text is made.
    /// </summary>
    /// <param name="builder">The pending exposition text. Cleared on return.</param>
    /// <param name="encoder">The response-scoped encoder, which carries surrogate state across flushes.</param>
    /// <param name="buffer">The pooled destination buffer.</param>
    /// <param name="destination">The stream to write to.</param>
    /// <param name="final">Whether this is the last flush, so the encoder should be drained.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    private static async Task FlushAsync(
        StringBuilder builder,
        Encoder encoder,
        byte[] buffer,
        Stream destination,
        bool final,
        CancellationToken cancellationToken)
    {
        foreach (var chunk in builder.GetChunks())
        {
            var pending = chunk;
            while (!pending.IsEmpty)
            {
                int charsUsed;
                int bytesUsed;
                // Convert (rather than GetBytes) lets the byte buffer stay small and
                // fixed no matter how large the chunk is: it converts as much as fits
                // and reports how far it got, so the loop drains the chunk in bounded
                // slices.
                encoder.Convert(
                    pending.Span, buffer.AsSpan(), flush: false,
                    out charsUsed, out bytesUsed, out _);

                if (bytesUsed > 0)
                {
                    await destination.WriteAsync(buffer.AsMemory(0, bytesUsed), cancellationToken)
                        .ConfigureAwait(false);
                }

                if (charsUsed == 0 && bytesUsed == 0)
                {
                    // Cannot happen with a buffer this size, but a zero-progress convert
                    // would otherwise spin forever; break rather than hang a scrape.
                    break;
                }

                pending = pending[charsUsed..];
            }
        }

        if (final)
        {
            // Drain any trailing high surrogate the encoder is holding. Without this a
            // body whose last char is an unpaired surrogate would silently lose its
            // replacement bytes.
            encoder.Convert(
                ReadOnlySpan<char>.Empty, buffer.AsSpan(), flush: true,
                out _, out var tailBytes, out _);
            if (tailBytes > 0)
            {
                await destination.WriteAsync(buffer.AsMemory(0, tailBytes), cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        builder.Clear();
    }

    /// <summary>
    /// Polls every observable instrument so a gauge reports its scrape-time value.
    /// </summary>
    private void PollObservableInstruments()
    {
        try
        {
            _listener.RecordObservableInstruments();
        }
        catch (ObjectDisposedException)
        {
            // The listener was disposed concurrently with a scrape; render what we hold.
        }
    }

    /// <summary>
    /// The families in the stable order both render paths emit them in.
    /// </summary>
    /// <returns>The families ordered by name.</returns>
    private IEnumerable<MetricFamily> OrderedFamilies() =>
        _families.Values.OrderBy(f => f.Name, StringComparer.Ordinal);

    /// <summary>
    /// Appends the collector's own self-report, which closes every exposition. Shared by
    /// both render paths so the streamed body cannot drift from the string one.
    /// </summary>
    /// <param name="builder">The builder to append to.</param>
    private void AppendTrailer(StringBuilder builder)
    {
        AppendMeta(builder, SeriesGaugeName, "gauge",
            "Distinct metric series currently held by the container's collector.",
            Interlocked.Read(ref _seriesCount));
        AppendMeta(builder, DroppedCounterName, "counter",
            "Measurements dropped because the container's collector reached a series ceiling, per family or overall.",
            Interlocked.Read(ref _dropped));
        AppendDropAttribution(builder);
        AppendSubscribedMeters(builder);
    }

    private void RecordMatchedMeter(string prefix, string meterName)
    {
        lock (_matchedMetersByPrefix)
        {
            _matchedMetersByPrefix[prefix].Add(meterName);
        }
    }

    private void AppendSubscribedMeters(StringBuilder builder)
    {
        builder.Append("# HELP ").Append(SubscribedMetersGaugeName)
            .Append(" Meter-name prefixes this collector subscribes to, valued with the number of distinct")
            .Append(" meters matched. A prefix absent here is not collected by this build; a prefix present")
            .Append(" at zero is collected and has published nothing.\n");
        builder.Append("# TYPE ").Append(SubscribedMetersGaugeName).Append(" gauge\n");

        lock (_matchedMetersByPrefix)
        {
            foreach (var prefix in SubscribedMeterNamePrefixes)
            {
                builder.Append(SubscribedMetersGaugeName)
                    .Append('{').Append(PrefixLabelName).Append("=\"")
                    .Append(RepoContextPrometheusExposition.EscapeLabelValue(prefix)).Append("\"} ")
                    .Append(_matchedMetersByPrefix[prefix].Count.ToString(CultureInfo.InvariantCulture))
                    .Append('\n');
            }
        }
    }

    private void AppendDropAttribution(StringBuilder builder)
    {
        if (_dropsByFamily.IsEmpty)
        {
            return;
        }

        builder.Append("# HELP ").Append(DroppedByFamilyCounterName)
            .Append(" Measurements dropped, attributed to the refused family and the ceiling that refused it.\n");
        builder.Append("# TYPE ").Append(DroppedByFamilyCounterName).Append(" counter\n");

        foreach (var entry in _dropsByFamily
                     .OrderBy(e => e.Key.Family, StringComparer.Ordinal)
                     .ThenBy(e => e.Key.Ceiling, StringComparer.Ordinal))
        {
            builder.Append(DroppedByFamilyCounterName)
                .Append('{').Append(FamilyLabelName).Append("=\"")
                .Append(RepoContextPrometheusExposition.EscapeLabelValue(entry.Key.Family)).Append("\",")
                .Append(CeilingLabelName).Append("=\"")
                .Append(RepoContextPrometheusExposition.EscapeLabelValue(entry.Key.Ceiling)).Append("\"} ")
                .Append(entry.Value.Read().ToString(CultureInfo.InvariantCulture))
                .Append('\n');
        }
    }

    private static void AppendMeta(StringBuilder builder, string name, string type, string help, long value)
    {
        builder.Append("# HELP ").Append(name).Append(' ').Append(help).Append('\n');
        builder.Append("# TYPE ").Append(name).Append(' ').Append(type).Append('\n');
        builder.Append(name).Append(' ').Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');
    }

    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        // Name comparison only - never a reference comparison against another
        // type's static Meter field. See the class remarks.
        var prefix = MatchPrefix(instrument.Meter.Name);
        if (prefix is null)
        {
            return;
        }

        RecordMatchedMeter(prefix, instrument.Meter.Name);

        var kind = KindOf(instrument);
        var name = RepoContextPrometheusExposition.MetricName(instrument.Name, kind);
        var family = _families.GetOrAdd(name, static (key, state) => new MetricFamily(key, state.Kind, state.Help),
            (Kind: kind, Help: BuildHelp(instrument)));

        // A second instrument mapping onto an existing name is only mergeable when
        // it renders as the same family; a counter and a histogram sharing a name
        // would emit two conflicting "# TYPE" lines and fail the whole scrape.
        if (family.Kind != kind)
        {
            return;
        }

        listener.EnableMeasurementEvents(instrument, family);
    }

    private static string BuildHelp(Instrument instrument)
    {
        var description = instrument.Description;
        var unit = instrument.Unit;
        var help = string.IsNullOrWhiteSpace(description) ? instrument.Name : description;
        return string.IsNullOrWhiteSpace(unit) ? help : $"{help} (unit: {unit})";
    }

    private void OnMeasurement<T>(
        Instrument instrument,
        T measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
        where T : struct
    {
        if (state is not MetricFamily family)
        {
            return;
        }

        var value = ToDouble(measurement);
        var labels = RenderLabels(tags);

        if (family.TryGetSeries(labels, out var series))
        {
            series.Record(instrument, family.Kind, value);
            return;
        }

        // A series this family has not seen before. The per-family ceiling is the
        // real cardinality control; the global one is only a memory backstop. Both
        // are checked before creation and never on the update path above, so a
        // series that already exists keeps reporting even while a ceiling is
        // refusing new ones. See the remarks on DefaultMaxSeriesPerFamily for why
        // the per-family ceiling has to come first.
        if (family.SeriesCount >= _maxSeriesPerFamily)
        {
            RecordDrop(family.Name, FamilyCeilingLabel);
            if (family.TryMarkCeilingReached())
            {
                CaptureCrossing(FamilyCeilingLabel, _maxSeriesPerFamily, family);
            }

            return;
        }

        if (Interlocked.Read(ref _seriesCount) >= _maxSeries)
        {
            RecordDrop(family.Name, GlobalCeilingLabel);
            if (Volatile.Read(ref _globalCeilingReached) == 0
                && Interlocked.CompareExchange(ref _globalCeilingReached, 1, 0) == 0)
            {
                CaptureCrossing(GlobalCeilingLabel, _maxSeries, family);
            }

            return;
        }

        if (family.AddSeries(labels, out series))
        {
            Interlocked.Increment(ref _seriesCount);
        }

        series.Record(instrument, family.Kind, value);
    }

    private void RecordDrop(string family, string ceiling)
    {
        Interlocked.Increment(ref _dropped);
        var count = _dropsByFamily.GetOrAdd((family, ceiling), static _ => new DropCount());
        Interlocked.Increment(ref count.Value);
    }

    /// <summary>
    /// Records a ceiling's first refusal at the instant it happens. Reached at most
    /// once per family plus once for the backstop, so the allocation here is off the
    /// per-measurement path entirely.
    /// </summary>
    private void CaptureCrossing(string ceiling, int limit, MetricFamily family)
    {
        _pendingCrossings.Enqueue(new CeilingCrossing(
            ceiling,
            limit,
            family.Name,
            family.SeriesCount,
            Interlocked.Read(ref _seriesCount),
            _timeProvider.GetUtcNow()));
        AnnouncePendingCrossings();
    }

    /// <summary>
    /// Writes every captured crossing to the attached logger, if there is one. Each
    /// crossing is dequeued by exactly one caller, so it is written exactly once even
    /// when a crossing races <see cref="AttachLogger"/>: the enqueue precedes the
    /// logger read here, and the logger write precedes the drain there, so at least
    /// one of the two sees the other's side.
    /// </summary>
    private void AnnouncePendingCrossings()
    {
        var logger = Volatile.Read(ref _logger);
        if (logger is null)
        {
            return;
        }

        while (_pendingCrossings.TryDequeue(out var crossing))
        {
            var log = string.Equals(crossing.Ceiling, GlobalCeilingLabel, StringComparison.Ordinal)
                ? LogGlobalCeilingReached
                : LogFamilyCeilingReached;
            log(
                logger,
                crossing.Ceiling,
                crossing.Limit,
                crossing.AtUtc,
                crossing.Family,
                crossing.FamilySeries,
                crossing.TotalSeries,
                null);
        }
    }

    /// <summary>
    /// Widens a measurement to <see cref="double"/> without boxing. Pattern
    /// matching on the generic value is specialised per instantiation, where
    /// <c>Convert.ToDouble(object, IFormatProvider)</c> would box on every
    /// measurement.
    /// </summary>
    private static double ToDouble<T>(T measurement)
        where T : struct
        => measurement switch
        {
            double d => d,
            long l => l,
            int i => i,
            float f => f,
            short s => s,
            byte b => b,
            decimal m => (double)m,
            _ => Convert.ToDouble(measurement, CultureInfo.InvariantCulture),
        };

    private static string RenderLabels(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
        {
            return string.Empty;
        }

        // A tagged measurement renders its label block on every record, which costs
        // a small allocation per measurement. That is a deliberate, bounded trade:
        // the alternative (a span-keyed series lookup that only materialises the
        // string on a cache miss) is meaningfully more code for a saving nothing
        // here has measured as material - this container's throughput is bound by
        // embedding round trips, not by counter bookkeeping. Revisit with a
        // measurement, not a hunch.

        // Sorted so a series key is stable regardless of the order the caller
        // happened to pass its tags in.
        var pairs = new (string Name, string Value)[tags.Length];
        for (var i = 0; i < tags.Length; i++)
        {
            pairs[i] = (
                RepoContextPrometheusExposition.SanitizeLabelName(tags[i].Key),
                RepoContextPrometheusExposition.EscapeLabelValue(
                    Convert.ToString(tags[i].Value, CultureInfo.InvariantCulture)));
        }

        Array.Sort(pairs, static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        var builder = new StringBuilder(tags.Length * 24);
        builder.Append('{');
        for (var i = 0; i < pairs.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(',');
            }

            builder.Append(pairs[i].Name).Append("=\"").Append(pairs[i].Value).Append('"');
        }

        return builder.Append('}').ToString();
    }

    /// <summary>Stops listening and releases the underlying <see cref="MeterListener"/>.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _listener.Dispose();
    }

    /// <summary>A mutable drop tally for one (family, ceiling) pair.</summary>
    private sealed class DropCount
    {
        public long Value;

        public long Read() => Interlocked.Read(ref Value);
    }

    /// <summary>
    /// One ceiling's first refusal, captured at the instant it happened: which
    /// ceiling, its configured value, the family being admitted, the family's and the
    /// collector's series counts, and when.
    /// </summary>
    private sealed record CeilingCrossing(
        string Ceiling,
        int Limit,
        string Family,
        long FamilySeries,
        long TotalSeries,
        DateTimeOffset AtUtc);

    /// <summary>One exposed metric family: a name, a Prometheus type, and its series.</summary>
    private sealed class MetricFamily(string name, RepoContextMetricKind kind, string help)
    {
        private readonly ConcurrentDictionary<string, Series> _series = new(StringComparer.Ordinal);
        private long _seriesCount;
        private int _ceilingReached;

        public string Name { get; } = name;

        public RepoContextMetricKind Kind { get; } = kind;

        /// <summary>
        /// Marks this family's per-family ceiling as reached, returning
        /// <see langword="true"/> to exactly one caller - the first refusal. The plain
        /// read first keeps every later refusal to a single volatile load, with no
        /// interlocked write on the saturated path.
        /// </summary>
        public bool TryMarkCeilingReached()
            => Volatile.Read(ref _ceilingReached) == 0
                && Interlocked.CompareExchange(ref _ceilingReached, 1, 0) == 0;

        /// <summary>
        /// The number of series this family holds. Tracked explicitly rather than
        /// read from the dictionary, because it is consulted on every measurement
        /// that misses the series lookup - which is precisely the hot path when a
        /// family is exploding - and <see cref="ConcurrentDictionary{TKey,TValue}.Count"/>
        /// acquires every bucket lock to answer.
        /// </summary>
        public long SeriesCount => Interlocked.Read(ref _seriesCount);

        public bool TryGetSeries(string labels, out Series series) => _series.TryGetValue(labels, out series!);

        public bool AddSeries(string labels, out Series series)
        {
            var created = new Series(labels);
            series = _series.GetOrAdd(labels, created);
            if (!ReferenceEquals(series, created))
            {
                return false;
            }

            Interlocked.Increment(ref _seriesCount);
            return true;
        }

        public void Render(StringBuilder builder)
        {
            builder.Append("# HELP ").Append(Name).Append(' ')
                .Append(RepoContextPrometheusExposition.EscapeHelp(help)).Append('\n');
            builder.Append("# TYPE ").Append(Name).Append(' ')
                .Append(RepoContextPrometheusExposition.TypeKeyword(Kind)).Append('\n');

            foreach (var series in _series.Values.OrderBy(s => s.Labels, StringComparer.Ordinal))
            {
                var (sum, count) = series.Read();
                if (Kind == RepoContextMetricKind.Summary)
                {
                    builder.Append(Name).Append("_sum").Append(series.Labels).Append(' ')
                        .Append(RepoContextPrometheusExposition.FormatValue(sum)).Append('\n');
                    builder.Append(Name).Append("_count").Append(series.Labels).Append(' ')
                        .Append(count.ToString(CultureInfo.InvariantCulture)).Append('\n');
                }
                else
                {
                    builder.Append(Name).Append(series.Labels).Append(' ')
                        .Append(RepoContextPrometheusExposition.FormatValue(sum)).Append('\n');
                }
            }
        }
    }

    /// <summary>One series within a family: its rendered label block and its accumulated value.</summary>
    private sealed class Series(string labels)
    {
        private readonly Lock _gate = new();
        private double _sum;
        private long _count;

        public string Labels { get; } = labels;

        public void Record(Instrument instrument, RepoContextMetricKind kind, double value)
        {
            // An observable instrument reports an ABSOLUTE value on every poll, so
            // summing its measurements would multiply it by the scrape count. A
            // synchronous instrument reports a DELTA, which must be summed. Getting
            // this the wrong way round is silent: the series still moves, just wrongly.
            var absolute = instrument.IsObservable && kind != RepoContextMetricKind.Summary;

            lock (_gate)
            {
                if (absolute)
                {
                    _sum = value;
                }
                else
                {
                    _sum += value;
                }

                _count++;
            }
        }

        public (double Sum, long Count) Read()
        {
            lock (_gate)
            {
                return (_sum, _count);
            }
        }
    }
}
