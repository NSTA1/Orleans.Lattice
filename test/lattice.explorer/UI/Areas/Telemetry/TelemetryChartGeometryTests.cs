using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// The chart layout's degenerate answers, which the rendered chart cannot tell
/// apart from a healthy one: a series with no finite reading, a timeline of one
/// instant, value ranges that are flat, all zero or wholly negative, and direct
/// labels crowding the plot's bottom edge.
/// </summary>
[TestFixture]
public sealed class TelemetryChartGeometryTests
{
    private const string Unit = "{op}/s";

    private static TelemetryChartGeometry Build(params TelemetryTimeSeries[] series) =>
        TelemetryChartGeometry.Build(
            new TelemetryQueryResponse { QueryId = "q", Scope = default, Series = series },
            Unit,
            TelemetryMeasurementSemantic.PerOperation);

    private static TelemetryTimeSeries Points(string tree, params TelemetryDataPoint[] points) => new()
    {
        Labels = [new TelemetryLabel("tree", tree)],
        Points = points,
    };

    [Test]
    public void A_series_with_no_finite_reading_is_not_charted()
    {
        var geometry = Build(Points("orders",
            new TelemetryDataPoint(TelemetryTestData.Now, double.NaN),
            new TelemetryDataPoint(TelemetryTestData.Now.AddMinutes(1), double.PositiveInfinity)));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.IsEmpty, Is.True);
            Assert.That(geometry.Lines, Is.Empty);
            Assert.That(geometry.IsTruncated, Is.False);
            Assert.That(geometry.Minimum, Is.EqualTo(0), "an empty extent defaults to the unit range");
            Assert.That(geometry.Maximum, Is.EqualTo(1));
            Assert.That(geometry.Times, Has.Count.EqualTo(2), "the timeline still spans every answered instant");
        });
    }

    [Test]
    public void A_series_with_no_finite_reading_is_dropped_beside_one_that_has_readings()
    {
        var geometry = Build(
            TelemetryTestData.Series("orders", TelemetryTestData.Now, 3, 4),
            Points("audit", new TelemetryDataPoint(TelemetryTestData.Now, double.NaN)));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Series.Select(series => series.Tree), Is.EqualTo(new[] { "orders" }));
            Assert.That(geometry.Lines, Has.Count.EqualTo(1));
            Assert.That(geometry.Maximum, Is.EqualTo(5), "4 rounds up to the next nice value");
        });
    }

    [Test]
    public void A_single_instant_is_centred_and_its_band_spans_the_plot()
    {
        var geometry = Build(TelemetryTestData.Series("orders", TelemetryTestData.Now, 7));
        var centre = (TelemetryChartGeometry.Left + TelemetryChartGeometry.PlotRight) / 2;

        Assert.Multiple(() =>
        {
            Assert.That(geometry.IsTimeSeries, Is.False);
            Assert.That(geometry.X(0), Is.EqualTo(centre));
            Assert.That(geometry.Band(0), Is.EqualTo((TelemetryChartGeometry.Left, TelemetryChartGeometry.PlotRight - TelemetryChartGeometry.Left)));
            Assert.That(geometry.Lines[0].Path, Does.StartWith("M" + TelemetryChartGeometry.Coordinate(centre) + " "));
        });
    }

    [Test]
    public void An_all_zero_answer_is_given_a_unit_range()
    {
        var geometry = Build(TelemetryTestData.Series("orders", TelemetryTestData.Now, 0, 0, 0));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Minimum, Is.EqualTo(0));
            Assert.That(geometry.Maximum, Is.EqualTo(1));
            Assert.That(geometry.Y(0), Is.EqualTo(TelemetryChartGeometry.PlotBottom), "a zero reading sits on the axis");
        });
    }

    [TestCase(0d, 1e307)]
    [TestCase(0d, double.MaxValue)]
    [TestCase(-double.MaxValue, double.MaxValue)]
    [TestCase(-double.MaxValue, -double.MaxValue)]
    [TestCase(0d, double.Epsilon)]
    public void Build_finite_extreme_readings_stay_inside_a_finite_plot(double first, double last)
    {
        var geometry = Build(TelemetryTestData.Series("orders", TelemetryTestData.Now, first, last));
        var floor = Math.Min(first, last);
        var ceiling = Math.Max(first, last);

        Assert.Multiple(() =>
        {
            Assert.That(double.IsFinite(geometry.Minimum), Is.True);
            Assert.That(double.IsFinite(geometry.Maximum), Is.True);
            Assert.That(geometry.Minimum, Is.LessThanOrEqualTo(floor));
            Assert.That(geometry.Maximum, Is.GreaterThanOrEqualTo(ceiling));
            Assert.That(geometry.Maximum, Is.GreaterThan(geometry.Minimum));
            Assert.That(geometry.Y(floor), Is.InRange(TelemetryChartGeometry.Top, TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Y(ceiling), Is.InRange(TelemetryChartGeometry.Top, TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Lines[0].LabelY, Is.InRange(TelemetryChartGeometry.Top, TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Lines[0].Path, Does.Not.Contain("NaN").And.Not.Contain("Infinity"));
            Assert.That(geometry.Ticks.All(tick => double.IsFinite(tick.Y)), Is.True);
        });
        if (floor < ceiling)
        {
            Assert.That(geometry.Y(ceiling), Is.LessThan(geometry.Y(floor)));
        }
    }

    [Test]
    public void A_wholly_negative_answer_keeps_its_floor_and_tops_out_at_one()
    {
        var geometry = Build(TelemetryTestData.Series("orders", TelemetryTestData.Now, -5, -5));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Minimum, Is.EqualTo(-5));
            Assert.That(geometry.Maximum, Is.EqualTo(1), "a non-positive ceiling is lifted to one");
            Assert.That(geometry.Y(-5), Is.EqualTo(TelemetryChartGeometry.PlotBottom));
            Assert.That(geometry.Ticks, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void A_timeline_crossing_midnight_carries_the_date()
    {
        var start = TelemetryTestData.Now.AddMinutes(-1);
        var geometry = Build(TelemetryTestData.Series("orders", start, 1, 2));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.SpansDays, Is.True);
            Assert.That(Build(TelemetryTestData.Series("orders", TelemetryTestData.Now, 1, 2)).SpansDays, Is.False);
        });
    }

    [Test]
    public void Labels_crowding_the_plot_bottom_are_pulled_back_up_inside_it()
    {
        var geometry = Build(
            TelemetryTestData.Series("audit", TelemetryTestData.Now, 10, 0),
            TelemetryTestData.Series("orders", TelemetryTestData.Now, 10, 0),
            TelemetryTestData.Series("scratch", TelemetryTestData.Now, 10, 0));
        var labels = geometry.Lines.Select(line => line.LabelY).Order().ToArray();
        var bottom = TelemetryChartGeometry.PlotBottom;
        var gap = TelemetryChartGeometry.LabelGap;

        Assert.That(labels, Is.EqualTo(new[] { bottom - (2 * gap), bottom - gap, bottom }));
    }

    [Test]
    public void Coordinates_are_invariant_and_carry_at_most_two_decimals()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryChartGeometry.Coordinate(1.234), Is.EqualTo("1.23"));
            Assert.That(TelemetryChartGeometry.Coordinate(264), Is.EqualTo("264"));
        });
    }

    [Test]
    public void Build_null_response_rejects_the_named_parameter()
    {
        var exception = Assert.Throws<ArgumentNullException>(() =>
            TelemetryChartGeometry.Build(null!, Unit, TelemetryMeasurementSemantic.PerOperation));

        Assert.That(exception!.ParamName, Is.EqualTo("response"));
    }

    [Test]
    public void Build_empty_answer_has_no_timeline_or_day_span()
    {
        var geometry = Build();

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Times, Is.Empty);
            Assert.That(geometry.Series, Is.Empty);
            Assert.That(geometry.Lines, Is.Empty);
            Assert.That(geometry.IsEmpty, Is.True);
            Assert.That(geometry.IsTimeSeries, Is.False);
            Assert.That(geometry.SpansDays, Is.False);
            Assert.That(geometry.IsTruncated, Is.False);
        });
    }

    [Test]
    public void Build_unsorted_offset_samples_align_on_a_shared_utc_timeline()
    {
        var start = TelemetryTestData.Now;
        var geometry = Build(
            Points("orders",
                new TelemetryDataPoint(start.AddMinutes(3), 9),
                new TelemetryDataPoint(start.ToOffset(TimeSpan.FromHours(2)), 1),
                new TelemetryDataPoint(start.AddMinutes(1), 4)),
            Points("audit", new TelemetryDataPoint(start.AddMinutes(1), 2)));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Times, Is.EqualTo(new[] { start, start.AddMinutes(1), start.AddMinutes(3) }));
            Assert.That(geometry.Times.Select(time => time.Offset), Is.All.EqualTo(TimeSpan.Zero));
            Assert.That(geometry.Series[0].Latest, Is.EqualTo(9), "latest is chronological, not the last input");
            Assert.That(geometry.Series[0].Values, Is.EqualTo(new[] { 1d, 4d, 9d }));
            Assert.That(geometry.Series[1].Values[0], Is.NaN);
            Assert.That(geometry.Series[1].Values[1], Is.EqualTo(2));
            Assert.That(geometry.Series[1].Values[2], Is.NaN);
            Assert.That(geometry.X(0), Is.EqualTo(64));
            Assert.That(geometry.X(1), Is.EqualTo(64 + 400d / 3).Within(0.0001));
            Assert.That(geometry.X(2), Is.EqualTo(464));
            Assert.That(geometry.Band(1).X, Is.EqualTo(64 + 200d / 3).Within(0.0001));
            Assert.That(geometry.Band(1).Width, Is.EqualTo(200).Within(0.0001));
        });
    }

    [Test]
    public void Build_duplicate_instants_keep_the_last_finite_sample_without_a_phantom_slot()
    {
        var start = TelemetryTestData.Now;
        var geometry = Build(Points("orders",
            new TelemetryDataPoint(start, 1),
            new TelemetryDataPoint(start.ToOffset(TimeSpan.FromHours(-5)), 7),
            new TelemetryDataPoint(start, double.NaN)));

        Assert.Multiple(() =>
        {
            Assert.That(geometry.Times, Has.Count.EqualTo(1));
            Assert.That(geometry.Series[0].Values, Is.EqualTo(new[] { 7d }));
            Assert.That(geometry.Series[0].Latest, Is.EqualTo(7));
            Assert.That(geometry.IsTimeSeries, Is.False);
        });
    }
}
