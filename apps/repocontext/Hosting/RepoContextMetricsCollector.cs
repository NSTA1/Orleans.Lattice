using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

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
    /// The case-insensitive meter-name prefix this collector subscribes to. Every
    /// Lattice meter is named under it - the core <c>orleans.lattice</c> meter and
    /// its per-package siblings, and the repository-context surface's own
    /// <c>Orleans.Lattice.Api.Mcp.RepoContext</c> meter - so one prefix covers the
    /// whole estate and a meter added by a future package is picked up without a
    /// code change here.
    /// </summary>
    public const string MeterNamePrefix = "orleans.lattice";

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

    private readonly ConcurrentDictionary<string, MetricFamily> _families = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Family, string Ceiling), DropCount> _dropsByFamily = new();
    private readonly MeterListener _listener = new();
    private readonly int _maxSeriesPerFamily;
    private readonly int _maxSeries;
    private long _seriesCount;
    private long _dropped;
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
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxSeriesPerFamily"/> or <paramref name="maxSeries"/> is not positive.
    /// </exception>
    public RepoContextMetricsCollector(
        int maxSeriesPerFamily = DefaultMaxSeriesPerFamily,
        int maxSeries = DefaultMaxSeries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSeriesPerFamily);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSeries);
        _maxSeriesPerFamily = maxSeriesPerFamily;
        _maxSeries = maxSeries;

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
    /// Whether the supplied meter name is one this collector subscribes to.
    /// </summary>
    /// <param name="meterName">The meter name to test.</param>
    /// <returns><see langword="true"/> when the meter is Lattice-owned.</returns>
    public static bool IsSubscribedMeter(string? meterName)
        => meterName is not null
           && meterName.StartsWith(MeterNamePrefix, StringComparison.OrdinalIgnoreCase);

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
    public string Render()
    {
        try
        {
            _listener.RecordObservableInstruments();
        }
        catch (ObjectDisposedException)
        {
            // The listener was disposed concurrently with a scrape; render what we hold.
        }

        var builder = new StringBuilder(4096);
        foreach (var family in _families.Values.OrderBy(f => f.Name, StringComparer.Ordinal))
        {
            family.Render(builder);
        }

        AppendMeta(builder, SeriesGaugeName, "gauge",
            "Distinct metric series currently held by the container's collector.",
            Interlocked.Read(ref _seriesCount));
        AppendMeta(builder, DroppedCounterName, "counter",
            "Measurements dropped because the container's collector reached a series ceiling, per family or overall.",
            Interlocked.Read(ref _dropped));
        AppendDropAttribution(builder);

        return builder.ToString();
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
        if (!IsSubscribedMeter(instrument.Meter.Name))
        {
            return;
        }

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
            return;
        }

        if (Interlocked.Read(ref _seriesCount) >= _maxSeries)
        {
            RecordDrop(family.Name, GlobalCeilingLabel);
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

    /// <summary>One exposed metric family: a name, a Prometheus type, and its series.</summary>
    private sealed class MetricFamily(string name, RepoContextMetricKind kind, string help)
    {
        private readonly ConcurrentDictionary<string, Series> _series = new(StringComparer.Ordinal);
        private long _seriesCount;

        public string Name { get; } = name;

        public RepoContextMetricKind Kind { get; } = kind;

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
