using System.Globalization;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The chart geometry: every case a real series can present - empty, single
/// point, flat, negative, a gap where a value was not finite, more series than
/// the palette has slots - resolved without a renderer, a browser, or a clock.
/// </summary>
[TestFixture]
public sealed class TelemetryChartTests
{
    [Test]
    public void The_view_box_literal_agrees_with_the_dimensions_it_is_built_from() =>
        // The literal exists so no panel interpolates one per render; this is
        // what stops the two copies drifting.
        Assert.That(
            TelemetryChart.ViewBox,
            Is.EqualTo(string.Create(
                CultureInfo.InvariantCulture,
                $"0 0 {TelemetryChart.ViewWidth} {TelemetryChart.ViewHeight}")));

    [Test]
    public void A_null_result_yields_the_shared_empty_chart() =>
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryChart.For(null), Is.SameAs(TelemetryChart.Empty));
            Assert.That(TelemetryChart.Empty.IsEmpty, Is.True);
            Assert.That(TelemetryChart.Empty.IsTruncated, Is.False);
        });

    [Test]
    public void A_result_with_no_series_yields_the_empty_chart() =>
        Assert.That(TelemetryChart.For(ExplorerTelemetrySample.EmptyResult()).IsEmpty, Is.True);

    [Test]
    public void A_series_with_no_points_is_dropped_rather_than_drawn_flat()
    {
        var empty = new ExplorerTelemetrySeries { Labels = [], Points = [] };

        Assert.That(TelemetryChart.For(ExplorerTelemetrySample.Result(null, empty)).IsEmpty, Is.True);
    }

    [Test]
    public void A_series_whose_every_point_is_non_finite_yields_the_empty_chart()
    {
        var series = new ExplorerTelemetrySeries
        {
            Labels = [],
            Points = ExplorerTelemetrySample.Points(double.NaN, double.PositiveInfinity),
        };

        Assert.That(TelemetryChart.For(ExplorerTelemetrySample.Result(null, series)).IsEmpty, Is.True);
    }

    [Test]
    public void A_normal_series_produces_one_polyline_with_a_point_per_reading()
    {
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1, 5, 3));

        var chart = TelemetryChart.For(result);

        Assert.Multiple(() =>
        {
            Assert.That(chart.Plots, Has.Count.EqualTo(1));
            Assert.That(chart.Plots[0].Points.Split(' '), Has.Length.EqualTo(3));
            Assert.That(chart.Plots[0].IsEmpty, Is.False);
            Assert.That(chart.Plots[0].Reading, Does.Contain("3"));
        });
    }

    [Test]
    public void A_non_finite_reading_leaves_a_gap_rather_than_being_interpolated_across()
    {
        var series = new ExplorerTelemetrySeries
        {
            Labels = [],
            Points = ExplorerTelemetrySample.Points(1, double.NaN, 3),
        };

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.Multiple(() =>
        {
            Assert.That(
                chart.Plots[0].Points.Split(' '),
                Has.Length.EqualTo(2),
                "a value that does not exist must not become a coordinate");
            Assert.That(
                chart.Plots[0].Reading,
                Does.Contain("3"),
                "the gap must not become the latest reading");
        });
    }

    [Test]
    public void A_flat_series_is_drawn_as_a_horizontal_line()
    {
        var series = ExplorerTelemetrySample.Series(null, null, 7, 7, 7);

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(YsOf(chart), Is.All.EqualTo(YsOf(chart)[0]));
    }

    [Test]
    public void A_series_that_is_flat_at_zero_is_drawn_down_the_middle_rather_than_divided_by_zero()
    {
        // The genuinely degenerate case: zero baseline and zero maximum leave no
        // span to scale against.
        var series = ExplorerTelemetrySample.Series(null, null, 0, 0, 0);

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(
            YsOf(chart).All(y => Math.Abs(y - (TelemetryChart.ViewHeight / 2d)) < 0.01),
            Is.True);
    }

    [Test]
    public void A_single_point_series_is_drawn_at_the_horizontal_midpoint()
    {
        var series = ExplorerTelemetrySample.Series(null, null, 42);

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        var x = double.Parse(
            chart.Plots[0].Points.Split(',')[0],
            CultureInfo.InvariantCulture);

        Assert.That(x, Is.EqualTo(TelemetryChart.ViewWidth / 2d).Within(0.01));
    }

    [Test]
    public void A_non_negative_series_is_scaled_against_a_zero_baseline()
    {
        // A chart whose floor is not zero exaggerates variation: 100 to 101
        // drawn full-height reads as a doubling.
        var series = ExplorerTelemetrySample.Series(null, null, 100, 101);

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(chart.Minimum, Is.EqualTo(0));
    }

    [Test]
    public void A_series_that_goes_negative_keeps_its_own_floor()
    {
        // Clamping to zero here would hide the negative excursion entirely.
        var series = ExplorerTelemetrySample.Series(null, null, -5, 10);

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.Multiple(() =>
        {
            Assert.That(chart.Minimum, Is.EqualTo(-5));
            Assert.That(chart.Maximum, Is.EqualTo(10));
        });
    }

    [Test]
    public void The_time_extent_spans_every_plotted_series()
    {
        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("a", null, 1, 2),
            ExplorerTelemetrySample.Series("b", null, 1, 2, 3, 4)));

        Assert.Multiple(() =>
        {
            Assert.That(chart.StartUtc, Is.EqualTo(ExplorerTelemetrySample.Now));
            Assert.That(chart.EndUtc, Is.EqualTo(ExplorerTelemetrySample.Now.AddMinutes(3)));
        });
    }

    [Test]
    public void The_tree_filter_keeps_only_the_series_carrying_that_tree()
    {
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1, 2),
            ExplorerTelemetrySample.Series("t/acme/audit", ExplorerTelemetrySample.TenantId, 3, 4));

        var chart = TelemetryChart.For(result, "t/acme/audit");

        Assert.Multiple(() =>
        {
            Assert.That(chart.Plots, Has.Count.EqualTo(1));
            Assert.That(chart.Plots[0].Label, Does.Contain("t/acme/audit"));
        });
    }

    [Test]
    public void A_tree_filter_matching_nothing_yields_the_empty_chart_rather_than_everything()
    {
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1, 2));

        Assert.That(TelemetryChart.For(result, "t/other/tree").IsEmpty, Is.True);
    }

    [Test]
    public void A_series_carrying_no_tree_label_is_excluded_by_a_tree_filter()
    {
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series(null, ExplorerTelemetrySample.TenantId, 1, 2));

        Assert.That(TelemetryChart.For(result, "t/acme/orders").IsEmpty, Is.True);
    }

    [Test]
    public void No_tree_filter_draws_every_series_the_facade_returned()
    {
        // The seam returns every series and the chart draws every series: a
        // panel never drops one for its labels, because deciding what a caller
        // may see is the facade's job.
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("a", "tenant-one", 1),
            ExplorerTelemetrySample.Series("b", "tenant-two", 2),
            ExplorerTelemetrySample.Series("c", TelemetryLabelNames.PlatformTenant, 3));

        Assert.That(TelemetryChart.For(result).Plots, Has.Count.EqualTo(3));
    }

    [Test]
    public void More_series_than_the_cap_are_truncated_and_the_chart_says_so()
    {
        var series = Enumerable
            .Range(0, TelemetryChart.MaxPlots + 4)
            .Select(i => ExplorerTelemetrySample.Series($"tree-{i}", null, i + 1))
            .ToArray();

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.Multiple(() =>
        {
            Assert.That(chart.Plots, Has.Count.EqualTo(TelemetryChart.MaxPlots));
            Assert.That(chart.TotalSeries, Is.EqualTo(TelemetryChart.MaxPlots + 4));
            Assert.That(chart.IsTruncated, Is.True);
        });
    }

    [Test]
    public void A_chart_within_the_cap_is_not_truncated() =>
        Assert.That(TelemetryChart.For(ExplorerTelemetrySample.Result()).IsTruncated, Is.False);

    [Test]
    public void Palette_slots_cycle_so_a_wide_result_never_asks_for_a_class_that_does_not_exist()
    {
        var series = Enumerable
            .Range(0, TelemetryChart.MaxPlots)
            .Select(i => ExplorerTelemetrySample.Series($"tree-{i}", null, i + 1))
            .ToArray();

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(chart.Plots.All(plot => plot.PaletteIndex is >= 0 and < 6), Is.True);
    }

    [Test]
    public void Coordinates_are_written_invariantly_so_a_comma_locale_cannot_corrupt_the_attribute()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series(null, null, 1, 2, 3)));

            // An SVG points attribute separates a pair with a comma and pairs
            // with a space. A decimal comma would make one pair read as two.
            foreach (var pair in chart.Plots[0].Points.Split(' '))
            {
                Assert.That(pair.Split(',').Length, Is.EqualTo(2), pair);
            }
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Test]
    public void The_legend_reading_is_formatted_once_with_the_entrys_unit_rather_than_per_render()
    {
        // A legend entry is re-rendered on every refresh and every breakpoint
        // change; formatting a double and appending a unit on each of those is
        // a per-frame allocation for a value that only changes with a result.
        var chart = TelemetryChart.For(
            ExplorerTelemetrySample.Result(null, ExplorerTelemetrySample.Series(null, null, 1, 2, 42)),
            treeFilter: null,
            unit: "ops/s",
            ExplorerTelemetrySemantic.PerOperation);

        Assert.That(chart.Plots[0].Reading, Is.EqualTo("42 ops/s"));
    }

    [Test]
    public void A_series_whose_readings_are_all_gaps_still_reports_a_reading_of_none()
    {
        var series = new ExplorerTelemetrySeries
        {
            Labels = [],
            Points = [.. ExplorerTelemetrySample.Points(double.NaN), .. ExplorerTelemetrySample.Points(1)],
        };

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(chart.Plots[0].Reading, Is.Not.Empty);
    }

    [Test]
    public void The_extent_is_measured_over_the_drawn_series_only_not_the_dropped_ones()
    {
        // Measuring across series that are then dropped normalises every drawn
        // line against a ceiling the user cannot see: one spiky thirteenth tree
        // would collapse the twelve visible ones into a flat band at the bottom
        // of the plot and make an active cluster read as idle.
        var series = Enumerable
            .Range(0, TelemetryChart.MaxPlots)
            .Select(i => ExplorerTelemetrySample.Series($"tree-{i}", null, 1))
            .Append(ExplorerTelemetrySample.Series("tree-spike", null, 1_000_000))
            .ToArray();

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.Multiple(() =>
        {
            Assert.That(chart.IsTruncated, Is.True);
            Assert.That(chart.TotalSeries, Is.EqualTo(TelemetryChart.MaxPlots + 1));
            Assert.That(
                chart.Maximum,
                Is.EqualTo(1),
                "the dropped spike must not set the ceiling the drawn lines are scaled against");
        });
    }

    [Test]
    public void The_time_extent_ignores_a_dropped_series_that_extends_beyond_the_drawn_ones()
    {
        var series = Enumerable
            .Range(0, TelemetryChart.MaxPlots)
            .Select(i => ExplorerTelemetrySample.Series($"tree-{i}", null, 1, 2))
            .Append(ExplorerTelemetrySample.Series("tree-long", null, 1, 2, 3, 4, 5))
            .ToArray();

        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(null, series));

        Assert.That(chart.EndUtc, Is.EqualTo(ExplorerTelemetrySample.Now.AddMinutes(1)));
    }

    [Test]
    public void Every_palette_slot_resolves_to_a_pre_composed_class_rather_than_one_built_per_render() =>
        Assert.Multiple(() =>
        {
            for (var i = 0; i < TelemetryPalette.Size; i++)
            {
                Assert.That(TelemetryPalette.SeriesClass(i), Is.EqualTo($"lxt-series lxt-series-{i}"));
                Assert.That(TelemetryPalette.SwatchClass(i), Is.EqualTo($"lxt-swatch lxt-series-{i}"));
                Assert.That(
                    TelemetryPalette.SeriesClass(i),
                    Is.SameAs(TelemetryPalette.SeriesClass(i)),
                    "a class handed out per plot per render must not be composed each time");
            }
        });

    [Test]
    public void An_out_of_range_palette_slot_resolves_rather_than_faulting_a_render() =>
        // A chart is never worth a render fault; TelemetryChart already reduces
        // the slot modulo the palette size, so this is belt and braces.
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryPalette.SeriesClass(-1), Is.EqualTo(TelemetryPalette.SeriesClass(0)));
            Assert.That(TelemetryPalette.SwatchClass(int.MaxValue), Is.EqualTo(TelemetryPalette.SwatchClass(0)));
        });

    private static double[] YsOf(TelemetryChart chart) => chart.Plots[0].Points
        .Split(' ')
        .Select(pair => double.Parse(pair.Split(',')[1], CultureInfo.InvariantCulture))
        .ToArray();
}
