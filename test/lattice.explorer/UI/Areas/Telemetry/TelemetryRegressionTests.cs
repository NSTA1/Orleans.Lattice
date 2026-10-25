using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TelemetryRegressionTests : TelemetryTestContext
{
    private static readonly TelemetryQueryDescriptor Reads = TelemetryTestData.Range("tree.read.operation_rate", "Read operations");
    private static readonly TelemetryQueryRequest Ask = new() { QueryId = Reads.QueryId };

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public void Series_with_the_same_display_name_render_without_duplicate_keys(bool range, bool table)
    {
        TelemetryTimeSeries Series(string instance, double value) => TelemetryTestData.Labelled(
            [new("tree", "orders"), new("tenant", "acme"), new("replica", "west"), new("instance", instance)],
            TelemetryTestData.Now,
            range ? [value, value + 1] : [value]);
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            Series("one", 1), Series("two", 10)));

        var cut = RenderChart(table);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Instance.Geometry!.Series, Has.Count.EqualTo(2));
            Assert.That(cut.Instance.Geometry.Series.Select(series => series.Name).Distinct().Count(), Is.EqualTo(1),
                "display names deliberately omit labels beyond the third");
            if (range && !table)
            {
                Assert.That(cut.FindAll(".lt-telemetry-legend__item"), Has.Count.EqualTo(2));
            }
            else
            {
                Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
                Assert.That(cut.Find("table").TextContent, Does.Contain(range ? "11 op/s" : "10 op/s"));
            }
        });

        cut.Render(parameters => parameters.Add(chart => chart.Generation, 1));
    }

    [TestCase("15s", 60)]
    [TestCase("5m", 300)]
    [TestCase("1h", 600)]
    public void A_step_without_a_window_reaches_the_facade_with_default_endpoints(string step, int expectedSeconds)
    {
        var bounds = new TelemetryQueryBounds
        {
            MinStep = TimeSpan.FromMinutes(1),
            MaxStep = TimeSpan.FromMinutes(10),
            DefaultStep = TimeSpan.FromMinutes(2),
        };
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry/throughput?step=" + step));
        var chart = TelemetryChartRequest.For(Reads with { Bounds = bounds }, window, TelemetryTestData.Now, false);

        Assert.Multiple(() =>
        {
            Assert.That(chart.Problem, Is.Null);
            Assert.That(chart.Request!.Range.Step, Is.EqualTo(TimeSpan.FromSeconds(expectedSeconds)));
            Assert.That(chart.Request.Range.StartUtc, Is.EqualTo(default(DateTimeOffset)));
            Assert.That(chart.Request.Range.EndUtc, Is.EqualTo(default(DateTimeOffset)));
        });
    }

    [Test]
    public void A_step_is_not_sent_to_instant_queries_or_queries_that_do_not_accept_it()
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry/throughput?step=5m"));
        var queries = new[]
        {
            TelemetryTestData.Instant("tree.storage.bytes", "Stored bytes"),
            Reads with { Parameters = TelemetryQueryParameters.TimeRange },
        };

        foreach (var query in queries)
        {
            var chart = TelemetryChartRequest.For(query, window, TelemetryTestData.Now, false);
            Assert.That(chart.Request!.Range, Is.EqualTo(default(TelemetryTimeRange)));
        }
    }

    [Test]
    public void Subminute_samples_have_distinct_times_in_the_table_and_keyboard_readout()
    {
        var start = TelemetryTestData.Now;
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            new TelemetryTimeSeries
            {
                Labels = [new("tree", "orders")],
                Points = [new(start, 1), new(start.AddSeconds(15), 2), new(start.AddSeconds(30), 3)],
            }));

        var table = RenderChart(table: true);
        table.WaitUntil(() => Assert.That(table.FindAll("tbody th").Select(cell => cell.TextContent.Trim()),
            Is.EqualTo(new[] { "00:00:00", "00:00:15", "00:00:30" })));

        var chart = RenderChart();
        chart.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "End" });
        Assert.Multiple(() =>
        {
            Assert.That(chart.Find(".lt-telemetry-readout").TextContent, Does.StartWith("00:00:30 UTC:"));
            Assert.That(chart.Find(".lt-telemetry-figure__caption").TextContent,
                Does.Contain("2026-01-01 00:00:00 to 00:00:30 UTC"));
            Assert.That(chart.FindAll(".lt-telemetry-svg__tick").Select(tick => tick.TextContent),
                Does.Contain("00:00:00").And.Contain("00:00:30 UTC"));
        });
    }

    [Test]
    public void Fractional_samples_across_midnight_keep_their_date_and_precision()
    {
        var start = TelemetryTestData.Now.AddMilliseconds(-500);
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            new TelemetryTimeSeries
            {
                Labels = [new("tree", "orders")],
                Points = [new(start, 1), new(start.AddMilliseconds(750), 2)],
            }));

        var cut = RenderChart(table: true);

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()),
            Is.EqualTo(new[] { "2025-12-31 23:59:59.5", "2026-01-01 00:00:00.25" })));
    }

    private IRenderedComponent<TelemetryChart> RenderChart(bool table = false) =>
        Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Request, Ask)
            .Add(chart => chart.ShowTable, table));
}
