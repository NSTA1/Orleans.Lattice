using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The whole geometry of one chart, computed once per response: the plotted
/// series, the value axis the points were scaled against, and the time axis
/// they were laid out along.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a pure function of a result, and is where the charting actually
/// lives.</b> The component that renders it does nothing but emit the strings
/// computed here, which is what lets every interesting case - an empty result, a
/// single point, a flat line, a series carrying <c>NaN</c> - be tested without a
/// renderer, a browser, or a clock.
/// </para>
/// <para>
/// <b>The view box is fixed and the SVG scales itself.</b> Coordinates are
/// computed in one nominal space and the element is sized by the stylesheet, so
/// the chart adapts to a phone by being drawn smaller rather than by the
/// component measuring anything. There is no width anywhere in this type
/// (epic decision D7).
/// </para>
/// </remarks>
public sealed record TelemetryChart
{
    /// <summary>The nominal view-box width the points are laid out across.</summary>
    public const int ViewWidth = 1000;

    /// <summary>The nominal view-box height the points are scaled into.</summary>
    public const int ViewHeight = 240;

    /// <summary>
    /// The <c>viewBox</c> attribute the chart component emits, as a literal so
    /// no panel interpolates one per render.
    /// </summary>
    /// <remarks>
    /// Restating the two dimensions is the price of it being a compile-time
    /// constant; <c>TelemetryChartTests</c> asserts it agrees with
    /// <see cref="ViewWidth"/> and <see cref="ViewHeight"/>, so the copies
    /// cannot drift.
    /// </remarks>
    public const string ViewBox = "0 0 1000 240";

    /// <summary>The vertical inset kept clear at the top and bottom of the plot.</summary>
    private const int VerticalPadding = 12;

    /// <summary>The palette size the stylesheet publishes classes for.</summary>
    private static readonly int PaletteSize = TelemetryPalette.Size;

    /// <summary>
    /// The most series a chart draws. Beyond this the legend stops being
    /// readable and the SVG stops being cheap, and a catalogue entry that
    /// returns hundreds of series is better answered by narrowing the tree
    /// filter than by drawing all of them.
    /// </summary>
    public const int MaxPlots = 12;

    private static readonly TelemetryPlot[] NoPlots = [];

    private static readonly TelemetryChart EmptyChart = new()
    {
        Plots = NoPlots,
        Minimum = 0,
        Maximum = 0,
        StartUtc = default,
        EndUtc = default,
        TotalSeries = 0,
    };

    /// <summary>The series that were plotted, in the order the backend produced them.</summary>
    public required IReadOnlyList<TelemetryPlot> Plots { get; init; }

    /// <summary>The lowest finite value across every plotted series.</summary>
    public required double Minimum { get; init; }

    /// <summary>The highest finite value across every plotted series.</summary>
    public required double Maximum { get; init; }

    /// <summary>The earliest timestamp across every plotted series.</summary>
    public required DateTimeOffset StartUtc { get; init; }

    /// <summary>The latest timestamp across every plotted series.</summary>
    public required DateTimeOffset EndUtc { get; init; }

    /// <summary>
    /// How many series the result carried, which can exceed
    /// <see cref="Plots"/>' count when the result was wider than
    /// <see cref="MaxPlots"/>.
    /// </summary>
    public required int TotalSeries { get; init; }

    /// <summary>The chart for a result with nothing to draw.</summary>
    public static TelemetryChart Empty => EmptyChart;

    /// <summary><see langword="true"/> when there is no geometry to render.</summary>
    public bool IsEmpty => Plots.Count == 0;

    /// <summary>
    /// <see langword="true"/> when lower-ranked series were dropped, so the
    /// panel can say so rather than silently charting a subset.
    /// </summary>
    public bool IsTruncated => TotalSeries > Plots.Count;

    /// <summary>
    /// Computes the geometry for <paramref name="result"/>, optionally keeping
    /// only the series carrying <paramref name="treeFilter"/> as their
    /// <c>tree</c> label.
    /// </summary>
    /// <remarks>
    /// The filter is a presentation convenience over series the facade already
    /// returned; it narrows what is drawn and can never widen it, and dropping
    /// it shows every series the facade served. Scope is never enforced here -
    /// that is the facade's job, and doing it on a desktop head is the
    /// bypassable path a routable facade exists to prevent.
    /// </remarks>
    /// <param name="result">The evaluated result, or <see langword="null"/> for an empty chart.</param>
    /// <param name="treeFilter">
    /// The single tree id to keep, or <see langword="null"/> to plot every
    /// series.
    /// </param>
    /// <param name="unit">
    /// The unit the catalogue entry published, appended to each legend reading
    /// here rather than in the component that renders it.
    /// </param>
    /// <param name="semantic">What one measurement counts, which decides how a reading is rounded.</param>
    /// <returns>The chart geometry.</returns>
    public static TelemetryChart For(
        ExplorerTelemetryResult? result,
        string? treeFilter = null,
        string? unit = null,
        ExplorerTelemetrySemantic semantic = ExplorerTelemetrySemantic.Unspecified)
    {
        if (result is null || result.Series.Count == 0)
        {
            return EmptyChart;
        }

        var kept = Select(result.Series, treeFilter);
        if (kept.Count == 0)
        {
            return EmptyChart;
        }

        var totalKept = kept.Count;

        // Truncated BEFORE the extent is measured, not after. Measuring across
        // series that are then dropped normalises every drawn line against a
        // ceiling the user cannot see: one spiky thirteenth tree would collapse
        // the twelve visible ones into a flat band at the bottom of the plot and
        // make an active cluster read as idle. TotalSeries still counts what was
        // kept, so the truncation notice stays honest.
        var drawn = Math.Min(kept.Count, MaxPlots);
        if (drawn < kept.Count)
        {
            kept.RemoveRange(drawn, kept.Count - drawn);
        }

        var extent = Extent.Measure(kept);
        if (!extent.HasPoints)
        {
            return EmptyChart;
        }

        var plots = new TelemetryPlot[kept.Count];
        for (var i = 0; i < plots.Length; i++)
        {
            plots[i] = Plot(kept[i], extent, i, unit, semantic);
        }

        return new TelemetryChart
        {
            Plots = plots,
            Minimum = extent.Minimum,
            Maximum = extent.Maximum,
            StartUtc = extent.StartUtc,
            EndUtc = extent.EndUtc,
            TotalSeries = totalKept,
        };
    }

    private static List<ExplorerTelemetrySeries> Select(
        IReadOnlyList<ExplorerTelemetrySeries> series,
        string? treeFilter)
    {
        var kept = new List<ExplorerTelemetrySeries>(series.Count);
        for (var i = 0; i < series.Count; i++)
        {
            var candidate = series[i];
            if (candidate.Points.Count == 0)
            {
                continue;
            }

            if (treeFilter is { Length: > 0 }
                && (!candidate.TryGetLabel(TelemetryLabelNames.Tree, out var tree)
                    || !string.Equals(tree, treeFilter, StringComparison.Ordinal)))
            {
                continue;
            }

            kept.Add(candidate);
        }

        return kept;
    }

    private static TelemetryPlot Plot(
        ExplorerTelemetrySeries series,
        Extent extent,
        int index,
        string? unit,
        ExplorerTelemetrySemantic semantic)
    {
        var points = series.Points;

        // Sized from the point count rather than grown: a range query returns
        // the same shape on every refresh, so one right-sized builder per series
        // replaces the log-n reallocations an unsized one would do per poll.
        var builder = new StringBuilder(points.Count * 12);
        double? latest = null;

        for (var i = 0; i < points.Count; i++)
        {
            var point = points[i];
            if (!point.IsFinite)
            {
                // A gap is a gap. Interpolating across it would draw a line
                // through data that does not exist.
                continue;
            }

            latest = point.Value;

            if (builder.Length > 0)
            {
                builder.Append(' ');
            }

            builder.Append(extent.X(point.TimestampUtc).ToString("0.##", CultureInfo.InvariantCulture));
            builder.Append(',');
            builder.Append(extent.Y(point.Value).ToString("0.##", CultureInfo.InvariantCulture));
        }

        return new TelemetryPlot(
            TelemetrySeriesLegend.LabelFor(series, index),
            builder.ToString(),
            index % PaletteSize,
            TelemetryValueFormat.WithUnit(latest, semantic, unit));
    }

    /// <summary>
    /// The value and time extents of a set of series, and the projection from a
    /// point to view-box coordinates.
    /// </summary>
    private readonly struct Extent
    {
        private readonly double _valueSpan;
        private readonly double _timeSpanTicks;

        private Extent(
            double minimum,
            double maximum,
            DateTimeOffset startUtc,
            DateTimeOffset endUtc,
            bool hasPoints)
        {
            Minimum = minimum;
            Maximum = maximum;
            StartUtc = startUtc;
            EndUtc = endUtc;
            HasPoints = hasPoints;

            // A flat series has no span to scale against, so it is drawn down
            // the middle rather than divided by zero or pinned to an edge. The
            // same applies to a single-point series in time.
            _valueSpan = maximum - minimum;
            _timeSpanTicks = (endUtc - startUtc).Ticks;
        }

        public double Minimum { get; }

        public double Maximum { get; }

        public DateTimeOffset StartUtc { get; }

        public DateTimeOffset EndUtc { get; }

        public bool HasPoints { get; }

        public static Extent Measure(List<ExplorerTelemetrySeries> series)
        {
            var minimum = double.MaxValue;
            var maximum = double.MinValue;
            var start = DateTimeOffset.MaxValue;
            var end = DateTimeOffset.MinValue;
            var any = false;

            for (var i = 0; i < series.Count; i++)
            {
                var points = series[i].Points;
                for (var p = 0; p < points.Count; p++)
                {
                    var point = points[p];
                    if (!point.IsFinite)
                    {
                        continue;
                    }

                    any = true;
                    if (point.Value < minimum)
                    {
                        minimum = point.Value;
                    }

                    if (point.Value > maximum)
                    {
                        maximum = point.Value;
                    }

                    if (point.TimestampUtc < start)
                    {
                        start = point.TimestampUtc;
                    }

                    if (point.TimestampUtc > end)
                    {
                        end = point.TimestampUtc;
                    }
                }
            }

            if (!any)
            {
                return new Extent(0, 0, default, default, hasPoints: false);
            }

            // A chart whose floor is not zero exaggerates variation, so a series
            // that never goes negative is drawn against a zero baseline. One
            // that does keeps its own floor, because clamping it to zero would
            // hide the negative excursion entirely.
            return new Extent(Math.Min(minimum, 0), maximum, start, end, hasPoints: true);
        }

        public double X(DateTimeOffset timestamp) => _timeSpanTicks <= 0
            ? ViewWidth / 2d
            : (timestamp - StartUtc).Ticks / (double)_timeSpanTicks * ViewWidth;

        public double Y(double value)
        {
            if (_valueSpan <= 0)
            {
                return ViewHeight / 2d;
            }

            var normalised = (value - Minimum) / _valueSpan;
            var usable = ViewHeight - (VerticalPadding * 2);

            // SVG's y axis grows downward, so a high value is a small y.
            return ViewHeight - VerticalPadding - (normalised * usable);
        }
    }
}
