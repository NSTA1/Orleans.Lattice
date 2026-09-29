using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Shell.Areas.Telemetry;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Telemetry;

/// <summary>
/// One Telemetry chart: the order-diagram line chart (two pigments, dash patterns and
/// direct labels beyond two series, the marker only on the selected point), the
/// readings and values tables, and every state from loading to refusal.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
[SetCulture("en-US")]
public sealed class TelemetryChartTests : TelemetryTestContext
{
    private static readonly TelemetryQueryDescriptor Reads = TelemetryTestData.Range("tree.read.operation_rate", "Read operations");

    private static readonly TelemetryQueryRequest Ask = new() { QueryId = Reads.QueryId };

    [Test]
    public void Series_beyond_two_are_told_apart_by_dash_and_direct_label_not_by_a_further_hue()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 30, 40),
            TelemetryTestData.Series("a/crm/lines", TelemetryTestData.Now, 20, 30),
            TelemetryTestData.Series("a/crm/audit", TelemetryTestData.Now, 10, 20)));

        var cut = RenderChart();

        cut.WaitUntil(() =>
        {
            var lines = cut.FindAll("path.lt-telemetry-svg__line");
            Assert.That(lines, Has.Count.EqualTo(3));
            Assert.That(lines.Select(line => line.ClassList.Contains("lt-telemetry-series--ink")), Is.EqualTo(new[] { true, false, true }));
            Assert.That(lines.Select(line => line.GetAttribute("stroke-dasharray")), Is.EqualTo(new[] { null, null, "7 4" }));
            Assert.That(cut.FindAll(".lt-telemetry-svg__label").Select(label => label.TextContent),
                Is.EquivalentTo(new[] { "a/crm/orders", "a/crm/lines", "a/crm/audit" }));
            Assert.That(cut.FindAll(".lt-telemetry-legend__item .lt-visually-hidden").Select(name => name.TextContent),
                Is.EqualTo(new[] { "dark solid line:", "blue solid line:", "dark dashed line:" }));
            Assert.That(cut.Markup, Does.Not.Contain("card"));
        });
    }

    [Test]
    public void The_legend_links_each_tree_to_the_tree_filter_and_to_Data()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 1, 2)));

        var cut = RenderChart(treeHref: tree => "telemetry/throughput?tree=" + tree, dataHref: tree => "data/" + tree);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("a.lt-telemetry-legend__name").GetAttribute("href"), Is.EqualTo("telemetry/throughput?tree=a/crm/orders"));
            Assert.That(cut.Find("a.lt-telemetry-legend__data").GetAttribute("href"), Is.EqualTo("data/a/crm/orders"));
            Assert.That(cut.Find("a.lt-telemetry-legend__data").TextContent, Is.EqualTo("Open a/crm/orders in Data"));
            Assert.That(cut.Find(".lt-telemetry-legend__value").TextContent, Is.EqualTo("2 op/s"));
        });
    }

    [Test]
    public void More_series_than_can_be_drawn_are_named_and_left_to_the_table()
    {
        var series = Enumerable.Range(0, 11)
            .Select(index => TelemetryTestData.Series($"t{index:00}", TelemetryTestData.Now, index, index + 1))
            .ToArray();
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default, series));

        var cut = RenderChart();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("path.lt-telemetry-svg__line"), Has.Count.EqualTo(TelemetryChartGeometry.MaxDrawn));
            Assert.That(cut.FindAll(".lt-telemetry-svg__label").Select(label => label.TextContent), Does.Contain("t10").And.Not.Contain("t00"),
                "the largest latest readings are drawn");
            Assert.That(cut.Find(".lt-telemetry-figure .lt-telemetry-chart__note").TextContent, Does.Contain("Drawing the 8 largest of 11 series"));
            Assert.That(cut.FindAll("path.lt-telemetry-svg__line").Select(line => line.GetAttribute("stroke-dasharray")).Distinct().Count(), Is.EqualTo(4));
        });
    }

    [Test]
    public void Arrow_keys_select_a_time_and_only_the_selected_points_take_the_marker()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 10, 20, 30),
            TelemetryTestData.Series("a/crm/lines", TelemetryTestData.Now, 1, double.NaN, 3)));

        var cut = RenderChart();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-telemetry-plot"), Has.Count.EqualTo(1)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("circle"), Is.Empty, "nothing is marked until a time is selected");
            Assert.That(cut.Find(".lt-telemetry-plot").GetAttribute("tabindex"), Is.EqualTo("0"));
            Assert.That(cut.Find(".lt-telemetry-plot").GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-telemetry-readout").Id));
            Assert.That(cut.Find(".lt-telemetry-readout").GetAttribute("aria-live"), Is.EqualTo("polite"));
        });

        cut.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "ArrowRight" });
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("circle[aria-current=\"true\"]"), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Is.EqualTo("00:02 UTC: a/crm/orders 30 op/s; a/crm/lines 3 op/s."));
        });

        cut.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "ArrowLeft" });
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("circle[aria-current=\"true\"]"), Has.Count.EqualTo(1), "a gap has no point to mark");
            Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Does.Contain("a/crm/lines no reading"));
        });

        cut.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "Home" });
        Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Does.StartWith("00:00 UTC"));

        cut.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "End" });
        Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Does.StartWith("00:02 UTC"));

        cut.Find(".lt-telemetry-plot").KeyDown(new KeyboardEventArgs { Key = "Escape" });
        Assert.That(cut.FindAll("circle"), Is.Empty);
    }

    [Test]
    public void Pointing_marks_the_hovered_time_and_a_tap_keeps_it()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 10, 20, 30)));

        var cut = RenderChart();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-telemetry-svg__band"), Has.Count.EqualTo(3)));

        cut.Find(".lt-telemetry-svg__band[data-time-index=\"1\"]").MouseOver();
        Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Does.StartWith("00:01 UTC"));

        cut.Find(".lt-telemetry-plot").MouseLeave();
        Assert.That(cut.FindAll("circle"), Is.Empty, "a hover leaves no mark behind");

        cut.Find(".lt-telemetry-svg__band[data-time-index=\"0\"]").Click();
        cut.Find(".lt-telemetry-svg__band[data-time-index=\"2\"]").MouseOver();
        cut.Find(".lt-telemetry-plot").MouseLeave();
        Assert.That(cut.Find(".lt-telemetry-readout").TextContent, Does.StartWith("00:00 UTC"), "a tapped time stays selected");
    }

    [Test]
    public void A_gap_lifts_the_line_rather_than_joining_across_it()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 1, double.NaN, 3, 4)));

        var cut = RenderChart();

        cut.WaitUntil(() => Assert.That(cut.Find("path.lt-telemetry-svg__line").GetAttribute("d")!.Split('M'), Has.Length.EqualTo(3)));
    }

    [Test]
    public void The_axes_are_hairlines_labelled_in_the_queries_unit_and_utc()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 0, 70)));

        var cut = RenderChart();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-telemetry-svg__grid"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll(".lt-telemetry-svg__tick").Select(tick => tick.TextContent),
                Is.EqualTo(new[] { "0 op/s", "50 op/s", "100 op/s", "00:00", "00:01 UTC" }));
            Assert.That(cut.Find("svg.lt-telemetry-svg").GetAttribute("aria-hidden"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-telemetry-figure__caption").TextContent, Does.Contain("1 series from 2026-01-01 00:00 to 00:01 UTC."));
            Assert.That(cut.Find(".lt-telemetry-chart__unit").TextContent, Is.EqualTo("op/s"));
        });
    }

    [Test]
    public void The_figure_links_to_its_table_alternative_and_the_table_view_reads_every_value()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 10, 20),
            TelemetryTestData.Series("a/crm/lines", TelemetryTestData.Now, 1, 2)));

        var chart = RenderChart(tableHref: "telemetry/throughput?view=table");
        chart.WaitUntil(() => Assert.That(chart.Find(".lt-telemetry-figure__caption a").GetAttribute("href"), Is.EqualTo("telemetry/throughput?view=table")));

        var table = RenderChart(showTable: true);
        table.WaitUntil(() =>
        {
            Assert.That(table.FindAll("svg.lt-telemetry-svg"), Is.Empty);
            Assert.That(table.FindAll("th[scope=col]").Select(header => header.TextContent.Trim()),
                Is.EqualTo(new[] { "Time (UTC)", "a/crm/orders", "a/crm/lines" }));
            Assert.That(table.FindAll("tbody tr"), Has.Count.EqualTo(2));
            Assert.That(table.FindAll("tbody tr")[1].TextContent, Does.Contain("00:01").And.Contain("20 op/s").And.Contain("2 op/s"));
        });
    }

    [Test]
    public void A_current_reading_is_a_table_of_readings_not_a_line()
    {
        var stored = TelemetryTestData.Instant("tree.storage.bytes", "Stored bytes");
        Telemetry.Answer(stored.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 1536),
            TelemetryTestData.Series("a/crm/lines", TelemetryTestData.Now, 3 * 1024 * 1024)));

        var cut = Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, stored)
            .Add(chart => chart.Request, new TelemetryQueryRequest { QueryId = stored.QueryId }));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("svg"), Is.Empty);
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].TextContent, Does.Contain("a/crm/lines").And.Contain("3 MiB"), "the largest reading first");
            Assert.That(rows[1].TextContent, Does.Contain("a/crm/orders").And.Contain("1.5 KiB"));
        });
    }

    [Test]
    public void At_compact_the_table_alternative_is_a_list_of_two_line_rows()
    {
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 10, 20)));

        var cut = Render<TelemetryChart>(parameters => parameters
            .AddCascadingValue(Orleans.Lattice.Explorer.Shell.Design.Components.LtBreakpointCascade.Name, Orleans.Lattice.Explorer.Shell.Design.Tokens.LtBreakpoint.Compact)
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Request, Ask)
            .Add(chart => chart.ShowTable, true));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo("00:00"));
        });
    }

    [Test]
    public void An_answer_with_no_series_says_so()
    {
        var cut = RenderChart();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-telemetry-chart__empty").TextContent, Is.EqualTo("No series matched this window.")));
    }

    [Test]
    public void The_chart_shows_a_skeleton_until_its_answer_arrives()
    {
        var pending = new TaskCompletionSource<TelemetryQueryResponse>();
        Telemetry.Pending(Reads.QueryId, pending);

        var cut = RenderChart();
        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));

        cut.InvokeAsync(() => pending.SetResult(TelemetryTestData.Response(Ask, default, TelemetryTestData.Series("t", TelemetryTestData.Now, 1, 2))));

        cut.WaitUntil(() => Assert.That(cut.FindAll("path.lt-telemetry-svg__line"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_backend_that_did_not_answer_is_retried_on_request()
    {
        Telemetry.Fail(Reads.QueryId, new TelemetryBackendException(Reads.QueryId, "down"));
        var cut = RenderChart();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("The metrics backend did not answer.")));

        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, default, TelemetryTestData.Series("t", TelemetryTestData.Now, 1, 2)));
        cut.Find("[role=alert] button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("path.lt-telemetry-svg__line"), Has.Count.EqualTo(1)));
        Assert.That(Telemetry.Requests, Has.Count.EqualTo(2));
    }

    [TestCase(typeof(NotSupportedException), "does not serve telemetry", false)]
    [TestCase(typeof(ArgumentException), "refused this chart's parameters", false)]
    [TestCase(typeof(InvalidOperationException), "could not be read", true)]
    public void Other_failures_stay_on_the_chart_with_a_retry_only_where_one_could_succeed(Type failure, string message, bool retry)
    {
        Telemetry.Fail(Reads.QueryId, (Exception)Activator.CreateInstance(failure, "detail")!);

        var cut = RenderChart();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain(message));
            Assert.That(cut.FindAll("[role=alert] button"), retry ? Has.Count.EqualTo(1) : Is.Empty);
        });
    }

    [Test]
    public void A_bounds_refusal_and_a_transient_transport_fault_are_explained()
    {
        Telemetry.Fail(Reads.QueryId, new TelemetryQueryBoundsException(Reads.QueryId, TelemetryBoundsViolation.RangeTooLong));
        var bounds = RenderChart();
        bounds.WaitUntil(() => Assert.That(bounds.Find("[role=alert]").TextContent, Does.Contain("would not draw this chart over that window")));

        Telemetry.Fail(Reads.QueryId, new ShellTransportException("gone", isTransient: true, new InvalidOperationException()));
        var transport = RenderChart();
        transport.WaitUntil(() => Assert.That(transport.Find("[role=alert]").TextContent, Does.Contain("The cluster did not answer.")));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void A_refused_query_is_reported_to_the_board_not_shown_as_an_error(bool notFound)
    {
        Telemetry.Fail(Reads.QueryId, notFound
            ? new TelemetryQueryNotFoundException(Reads.QueryId)
            : new UnauthorizedAccessException("denied"));
        string? denied = null;

        var cut = Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Request, Ask)
            .Add(chart => chart.OnDenied, id => denied = id));

        cut.WaitUntil(() =>
        {
            Assert.That(denied, Is.EqualTo(Reads.QueryId));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [Test]
    public void A_window_the_entry_cannot_draw_is_explained_and_nothing_is_asked()
    {
        var cut = Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Problem, "This chart covers at most 1 day. Choose a shorter range.")
            .Add(chart => chart.Note, "A note."));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-telemetry-chart__problem").TextContent, Is.EqualTo("This chart covers at most 1 day. Choose a shorter range."));
            Assert.That(cut.Find(".lt-telemetry-chart__note").TextContent, Is.EqualTo("A note."));
            Assert.That(Telemetry.Requests, Is.Empty);
        });
    }

    [Test]
    public void The_applied_scope_is_reported_and_a_new_generation_re_asks()
    {
        var scope = TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants);
        Telemetry.Answer(Reads.QueryId, request => TelemetryTestData.Response(request, scope));
        var reported = new List<TelemetryTenantScope>();

        var cut = Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Request, Ask)
            .Add(chart => chart.OnScope, value => reported.Add(value)));
        cut.WaitUntil(() => Assert.That(reported, Is.EqualTo(new[] { scope })));

        cut.Render(parameters => parameters.Add(chart => chart.Generation, 1));

        cut.WaitUntil(() => Assert.That(Telemetry.Requests, Has.Count.EqualTo(2)));
        cut.Render(parameters => parameters.Add(chart => chart.Generation, 1));
        Assert.That(Telemetry.Requests, Has.Count.EqualTo(2), "the same request and generation is not asked twice");
    }

    [Test]
    public void The_chart_heading_names_its_section_and_its_text_renders_as_text()
    {
        var hostile = Reads with { Title = "<img src=x onerror=alert(1)>", Description = "<b>bold</b>" };

        var cut = Render<TelemetryChart>(parameters => parameters.Add(chart => chart.Descriptor, hostile).Add(chart => chart.Request, Ask));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("section").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find("h2").Id));
            Assert.That(cut.FindAll("img"), Is.Empty);
            Assert.That(cut.FindAll("b"), Is.Empty);
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("<img src=x onerror=alert(1)>"));
        });
    }

    private IRenderedComponent<TelemetryChart> RenderChart(
        bool showTable = false,
        string? tableHref = null,
        Func<string, string>? treeHref = null,
        Func<string, string>? dataHref = null) =>
        Render<TelemetryChart>(parameters => parameters
            .Add(chart => chart.Descriptor, Reads)
            .Add(chart => chart.Request, Ask)
            .Add(chart => chart.ShowTable, showTable)
            .Add(chart => chart.TableHref, tableHref)
            .Add(chart => chart.TreeHref, treeHref)
            .Add(chart => chart.DataHref, dataHref));
}
