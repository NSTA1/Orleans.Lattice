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
    /// The default ceiling on distinct exposed series. A tag value the collector
    /// did not anticipate could otherwise grow the exposition without bound, so the
    /// cap fails closed: measurements beyond it are dropped and counted rather than
    /// retained.
    /// </summary>
    public const int DefaultMaxSeries = 10_000;

    /// <summary>The gauge reporting how many series the collector currently holds.</summary>
    public const string SeriesGaugeName = "lattice_metrics_series";

    /// <summary>The counter reporting measurements dropped because the series cap was reached.</summary>
    public const string DroppedCounterName = "lattice_metrics_dropped_measurements_total";

    private readonly ConcurrentDictionary<string, MetricFamily> _families = new(StringComparer.Ordinal);
    private readonly MeterListener _listener = new();
    private readonly int _maxSeries;
    private long _seriesCount;
    private long _dropped;
    private int _disposed;

    /// <summary>
    /// Creates a collector and starts listening. Instruments already published by
    /// the process are replayed by <see cref="MeterListener.Start"/>, so
    /// construction order relative to the metrics classes does not matter.
    /// </summary>
    /// <param name="maxSeries">
    /// The ceiling on distinct exposed series; defaults to
    /// <see cref="DefaultMaxSeries"/>. Must be positive.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxSeries"/> is not positive.</exception>
    public RepoContextMetricsCollector(int maxSeries = DefaultMaxSeries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSeries);
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
            "Measurements dropped because the container's collector reached its series ceiling.",
            Interlocked.Read(ref _dropped));

        return builder.ToString();
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

        if (Interlocked.Read(ref _seriesCount) >= _maxSeries)
        {
            Interlocked.Increment(ref _dropped);
            return;
        }

        if (family.AddSeries(labels, out series))
        {
            Interlocked.Increment(ref _seriesCount);
        }

        series.Record(instrument, family.Kind, value);
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

    /// <summary>One exposed metric family: a name, a Prometheus type, and its series.</summary>
    private sealed class MetricFamily(string name, RepoContextMetricKind kind, string help)
    {
        private readonly ConcurrentDictionary<string, Series> _series = new(StringComparer.Ordinal);

        public string Name { get; } = name;

        public RepoContextMetricKind Kind { get; } = kind;

        public bool TryGetSeries(string labels, out Series series) => _series.TryGetValue(labels, out series!);

        public bool AddSeries(string labels, out Series series)
        {
            var created = new Series(labels);
            series = _series.GetOrAdd(labels, created);
            return ReferenceEquals(series, created);
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
