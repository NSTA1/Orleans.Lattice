using System.Globalization;
using System.Text;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// A telemetry answer laid out for the order-diagram chart: the shared timeline,
/// every series in reading order, the at most <see cref="MaxDrawn"/> series drawn
/// as lines (two pigments, told apart beyond two by dash pattern and a direct
/// label, never by a further hue), hairline axis ticks, and the direct labels'
/// positions. Computed once per answer, never per render.
/// </summary>
internal sealed class TelemetryChartGeometry
{
    /// <summary>The chart's view-box width.</summary>
    public const double Width = 640;

    /// <summary>The chart's view-box height.</summary>
    public const double Height = 260;

    /// <summary>The left margin, which holds the value axis labels.</summary>
    public const double Left = 64;

    /// <summary>The right margin, which holds the direct labels.</summary>
    public const double Right = 176;

    /// <summary>The top margin.</summary>
    public const double Top = 14;

    /// <summary>The bottom margin, which holds the time axis labels.</summary>
    public const double Bottom = 30;

    /// <summary>The most series drawn as lines: two pigments times four dash patterns.</summary>
    public const int MaxDrawn = 8;

    /// <summary>The longest direct label, in characters, before it is shortened.</summary>
    public const int MaxLabelLength = 22;

    /// <summary>The least vertical distance between two direct labels.</summary>
    public const double LabelGap = 13;

    /// <summary>The dash patterns, in the order the series pairs take them; the first is solid.</summary>
    public static IReadOnlyList<string> DashPatterns { get; } = [string.Empty, "7 4", "2 3", "9 3 2 3"];

    private TelemetryChartGeometry(
        IReadOnlyList<DateTimeOffset> times,
        IReadOnlyList<TelemetrySeriesView> series,
        IReadOnlyList<TelemetryPlotLine> lines,
        IReadOnlyList<TelemetryAxisTick> ticks,
        double minimum,
        double maximum)
    {
        Times = times;
        Series = series;
        Lines = lines;
        Ticks = ticks;
        Minimum = minimum;
        Maximum = maximum;
    }

    /// <summary>The shared timeline every series is aligned to, ascending.</summary>
    public IReadOnlyList<DateTimeOffset> Times { get; }

    /// <summary>Every series in the answer that carries a reading, largest latest reading first.</summary>
    public IReadOnlyList<TelemetrySeriesView> Series { get; }

    /// <summary>The series drawn as lines: the first <see cref="MaxDrawn"/> of <see cref="Series"/>.</summary>
    public IReadOnlyList<TelemetryPlotLine> Lines { get; }

    /// <summary>The value axis ticks, bottom first.</summary>
    public IReadOnlyList<TelemetryAxisTick> Ticks { get; }

    /// <summary>The value at the bottom of the plot.</summary>
    public double Minimum { get; }

    /// <summary>The value at the top of the plot.</summary>
    public double Maximum { get; }

    /// <summary>Whether the answer is a line over time rather than a set of single readings.</summary>
    public bool IsTimeSeries => Times.Count >= 2;

    /// <summary>Whether no series carries a reading.</summary>
    public bool IsEmpty => Series.Count == 0;

    /// <summary>Whether more series were answered than are drawn.</summary>
    public bool IsTruncated => Series.Count > Lines.Count;

    /// <summary>The plot's right edge.</summary>
    public static double PlotRight => Width - Right;

    /// <summary>The plot's bottom edge.</summary>
    public static double PlotBottom => Height - Bottom;

    /// <summary>Whether the timeline spans more than one day, so its labels carry the date.</summary>
    public bool SpansDays => Times.Count > 0 && Times[^1].UtcDateTime.Date != Times[0].UtcDateTime.Date;

    /// <summary>Lays out <paramref name="response"/>.</summary>
    /// <param name="response">The facade's answer.</param>
    /// <param name="unit">The query's unit, for tick labels.</param>
    /// <param name="semantic">What the query measures, for tick labels.</param>
    /// <returns>The layout.</returns>
    public static TelemetryChartGeometry Build(TelemetryQueryResponse response, string? unit, TelemetryMeasurementSemantic semantic)
    {
        ArgumentNullException.ThrowIfNull(response);

        var times = response.Series
            .SelectMany(series => series.Points)
            .Select(point => point.Timestamp.ToUniversalTime())
            .Distinct()
            .Order()
            .ToArray();
        var slot = new Dictionary<DateTimeOffset, int>(times.Length);
        for (var i = 0; i < times.Length; i++)
        {
            slot[times[i]] = i;
        }

        var views = new List<TelemetrySeriesView>(response.Series.Count);
        for (var s = 0; s < response.Series.Count; s++)
        {
            var source = response.Series[s];
            var values = new double[times.Length];
            Array.Fill(values, double.NaN);
            var latest = double.NaN;
            var latestAt = DateTimeOffset.MinValue;
            foreach (var point in source.Points)
            {
                if (!point.IsFinite)
                {
                    continue;
                }

                var at = point.Timestamp.ToUniversalTime();
                values[slot[at]] = point.Value;
                if (at >= latestAt)
                {
                    latestAt = at;
                    latest = point.Value;
                }
            }

            if (double.IsNaN(latest))
            {
                continue;
            }

            var (name, tree) = TelemetrySeriesView.Describe(source, s);
            views.Add(new TelemetrySeriesView(name, tree, values, latest));
        }

        var ordered = views
            .OrderByDescending(view => view.Latest)
            .ThenBy(view => view.Name, StringComparer.Ordinal)
            .ToArray();

        var drawn = ordered.Take(MaxDrawn).ToArray();
        var finite = drawn.SelectMany(view => view.Values).Where(double.IsFinite).ToArray();
        var (minimum, maximum) = Extent(finite);
        var ticks = new[] { minimum, (minimum + maximum) / 2, maximum }
            .Select(value => new TelemetryAxisTick(Y(value, minimum, maximum), TelemetryFormat.Value(value, unit, semantic)))
            .ToArray();

        var lines = new TelemetryPlotLine[drawn.Length];
        for (var i = 0; i < drawn.Length; i++)
        {
            lines[i] = new TelemetryPlotLine(
                drawn[i],
                i,
                Ink: i % 2 == 0,
                Dash: DashPatterns[i / 2],
                Path: PathOf(drawn[i].Values, times, minimum, maximum),
                LabelY: 0,
                Label: Shorten(drawn[i].Name));
        }

        return new TelemetryChartGeometry(times, ordered, PlaceLabels(lines, minimum, maximum), ticks, minimum, maximum);
    }

    /// <summary>The horizontal position of the timeline's <paramref name="index"/>th time.</summary>
    /// <param name="index">The time's index.</param>
    /// <returns>The x coordinate.</returns>
    public double X(int index) => X(Times, index);

    /// <summary>The vertical position of <paramref name="value"/>.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The y coordinate.</returns>
    public double Y(double value) => Y(value, Minimum, Maximum);

    /// <summary>The horizontal extent of the hit band around the timeline's <paramref name="index"/>th time.</summary>
    /// <param name="index">The time's index.</param>
    /// <returns>The band's left edge and width.</returns>
    public (double X, double Width) Band(int index)
    {
        var left = index == 0 ? Left : (X(index - 1) + X(index)) / 2;
        var right = index == Times.Count - 1 ? PlotRight : (X(index) + X(index + 1)) / 2;
        return (left, Math.Max(1, right - left));
    }

    /// <summary>Formats a coordinate for SVG.</summary>
    /// <param name="value">The coordinate.</param>
    /// <returns>The text.</returns>
    public static string Coordinate(double value) => value.ToString("0.##", CultureInfo.InvariantCulture);

    private static double X(IReadOnlyList<DateTimeOffset> times, int index)
    {
        if (times.Count < 2)
        {
            return (Left + PlotRight) / 2;
        }

        var span = (times[^1] - times[0]).Ticks;
        return Left + ((PlotRight - Left) * (times[index] - times[0]).Ticks / span);
    }

    private static double Y(double value, double minimum, double maximum) =>
        PlotBottom - ((PlotBottom - Top) * (value - minimum) / (maximum - minimum));

    private static (double Minimum, double Maximum) Extent(double[] values)
    {
        if (values.Length == 0)
        {
            return (0, 1);
        }

        var low = Math.Min(0, values.Min());
        var high = values.Max();
        if (high <= low)
        {
            high = low + 1;
        }

        return (low, Nice(high));
    }

    private static double Nice(double value)
    {
        if (value <= 0)
        {
            return 1;
        }

        var magnitude = Math.Pow(10, Math.Floor(Math.Log10(value)));
        foreach (var factor in new[] { 1d, 2d, 2.5, 5d, 10d })
        {
            if (factor * magnitude >= value)
            {
                return factor * magnitude;
            }
        }

        return 10 * magnitude;
    }

    private static string PathOf(IReadOnlyList<double> values, IReadOnlyList<DateTimeOffset> times, double minimum, double maximum)
    {
        var builder = new StringBuilder(values.Count * 14);
        var pen = false;
        for (var i = 0; i < values.Count; i++)
        {
            if (!double.IsFinite(values[i]))
            {
                // A gap is a gap: the line lifts rather than joining across time
                // with no reading.
                pen = false;
                continue;
            }

            builder.Append(pen ? " L" : (builder.Length == 0 ? "M" : " M"))
                .Append(Coordinate(X(times, i)))
                .Append(' ')
                .Append(Coordinate(Y(values[i], minimum, maximum)));
            pen = true;
        }

        return builder.ToString();
    }

    private static TelemetryPlotLine[] PlaceLabels(TelemetryPlotLine[] lines, double minimum, double maximum)
    {
        var wanted = lines
            .Select(line => (Line: line, Y: Y(line.Series.Latest, minimum, maximum)))
            .OrderBy(entry => entry.Y)
            .ToArray();

        var placed = new double[wanted.Length];
        for (var i = 0; i < wanted.Length; i++)
        {
            placed[i] = Math.Max(wanted[i].Y, i == 0 ? Top + 4 : placed[i - 1] + LabelGap);
        }

        var overflow = placed.Length == 0 ? 0 : placed[^1] - PlotBottom;
        for (var i = placed.Length - 1; i >= 0 && overflow > 0; i--)
        {
            var limit = i == placed.Length - 1 ? PlotBottom : placed[i + 1] - LabelGap;
            placed[i] = Math.Min(placed[i], limit);
        }

        var result = new TelemetryPlotLine[lines.Length];
        for (var i = 0; i < wanted.Length; i++)
        {
            result[wanted[i].Line.Index] = wanted[i].Line with { LabelY = placed[i] };
        }

        return result;
    }

    private static string Shorten(string name) =>
        name.Length <= MaxLabelLength ? name : string.Concat(name.AsSpan(0, MaxLabelLength - 1), "\u2026");
}
