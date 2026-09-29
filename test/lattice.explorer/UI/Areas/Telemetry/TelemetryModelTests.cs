using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// The Telemetry area's model: the durations and window the address carries, the
/// request each chart sends under it, the boards resolved against a catalogue, and
/// how readings, series and scopes are written.
/// </summary>
[TestFixture]
[SetCulture("en-US")]
public sealed class TelemetryModelTests
{
    [TestCase("15m", 15 * 60)]
    [TestCase("1h", 3600)]
    [TestCase("30s", 30)]
    [TestCase("7d", 7 * 86400)]
    public void Durations_round_trip_through_the_address(string token, int seconds)
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryDurations.TryParse(token, out var duration), Is.True);
            Assert.That(duration, Is.EqualTo(TimeSpan.FromSeconds(seconds)));
            Assert.That(TelemetryDurations.Format(duration), Is.EqualTo(token));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("0m")]
    [TestCase("1w")]
    [TestCase("-1h")]
    [TestCase("1H")]
    [TestCase("999d")]
    public void A_duration_the_address_cannot_name_is_refused(string? token) =>
        Assert.That(TelemetryDurations.TryParse(token, out _), Is.False);

    [Test]
    public void Durations_read_as_labels_and_prose()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryDurations.Label(TimeSpan.FromMinutes(15)), Is.EqualTo("15 min"));
            Assert.That(TelemetryDurations.Label(TimeSpan.FromDays(7)), Is.EqualTo("7 d"));
            Assert.That(TelemetryDurations.Label(TimeSpan.FromSeconds(15)), Is.EqualTo("15 s"));
            Assert.That(TelemetryDurations.Describe(TimeSpan.FromHours(24)), Is.EqualTo("1 day"));
            Assert.That(TelemetryDurations.Describe(TimeSpan.FromMinutes(1)), Is.EqualTo("1 minute"));
            Assert.That(TelemetryDurations.Describe(TimeSpan.FromHours(6)), Is.EqualTo("6 hours"));
            Assert.That(TelemetryDurations.Ranges, Is.Ordered);
            Assert.That(TelemetryDurations.Steps, Is.Ordered);
            Assert.That(TelemetryDurations.AutomaticSteps, Is.Ordered);
        });
    }

    [Test]
    public void The_window_reads_every_parameter_the_address_carries()
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry/latency?range=6h&step=5m&tree=a%2Fcrm%2Forders&scope=all&view=table"));

        Assert.Multiple(() =>
        {
            Assert.That(window.Range, Is.EqualTo(TimeSpan.FromHours(6)));
            Assert.That(window.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(window.Tree, Is.EqualTo("a/crm/orders"));
            Assert.That(window.AllTenants, Is.True);
            Assert.That(window.ShowTable, Is.True);
            Assert.That(window.Notice, Is.Null);
            Assert.That(window.IsDefault, Is.False);
            Assert.That(window.IsAbsolute, Is.False);
            Assert.That(window.Resolve(TelemetryTestData.Now), Is.EqualTo((TelemetryTestData.Now.AddHours(-6), TelemetryTestData.Now)));
        });
    }

    [Test]
    public void An_absolute_window_wins_over_a_range_and_reads_both_instant_forms()
    {
        var basic = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry?from=20260101T100000Z&to=20260101T110000Z&range=1h"));
        var extended = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry?from=2026-01-01T10:00:00Z&to=2026-01-01T11:00Z"));
        var start = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(basic.IsAbsolute, Is.True);
            Assert.That(basic.Range, Is.Null);
            Assert.That(basic.Resolve(TelemetryTestData.Now), Is.EqualTo((start, start.AddHours(1))));
            Assert.That(extended.From, Is.EqualTo(start));
            Assert.That(extended.To, Is.EqualTo(start.AddHours(1)));
            Assert.That(TelemetryWindow.FormatInstant(start), Is.EqualTo("20260101T100000Z"));
        });
    }

    [TestCase("/telemetry?range=soon", "the time range")]
    [TestCase("/telemetry?from=20260101T110000Z&to=20260101T100000Z", "the time window")]
    [TestCase("/telemetry?from=yesterday", "the time window")]
    [TestCase("/telemetry?step=fast", "the step")]
    public void What_the_address_names_unreadably_is_dropped_with_a_notice(string address, string named)
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse(address));

        Assert.Multiple(() =>
        {
            Assert.That(window.Notice, Does.Contain(named));
            Assert.That(window.Resolve(TelemetryTestData.Now), Is.Null, "an unreadable window falls back to each chart's default");
        });
    }

    [Test]
    public void An_address_with_no_window_asks_each_chart_for_its_default()
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry/throughput"));
        var chart = TelemetryChartRequest.For(TelemetryTestData.Range("q", "Q"), window, TelemetryTestData.Now, tenancyActive: false);

        Assert.Multiple(() =>
        {
            Assert.That(window.IsDefault, Is.True);
            Assert.That(chart.Request!.Range, Is.EqualTo(default(TelemetryTimeRange)));
            Assert.That(chart.Request.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(chart.Problem, Is.Null);
            Assert.That(chart.Note, Is.Null);
        });
    }

    [Test]
    public void A_relative_range_becomes_a_concrete_window_with_an_automatic_step()
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry/throughput?range=1h"));
        var request = TelemetryChartRequest.For(TelemetryTestData.Range("q", "Q"), window, TelemetryTestData.Now, false).Request!;

        Assert.Multiple(() =>
        {
            Assert.That(request.Range.StartUtc, Is.EqualTo(TelemetryTestData.Now.AddHours(-1)));
            Assert.That(request.Range.EndUtc, Is.EqualTo(TelemetryTestData.Now));
            Assert.That(request.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(1)), "an hour at 15s would be 241 points, one over the target");
            Assert.That(TelemetryChartRequest.AutomaticStep(TimeSpan.FromDays(7), default), Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(TelemetryChartRequest.AutomaticStep(TimeSpan.FromDays(400), default), Is.EqualTo(TimeSpan.FromDays(1)));
        });
    }

    [Test]
    public void An_explicit_step_is_clamped_into_the_entrys_step_budget()
    {
        var bounds = new TelemetryQueryBounds { MinStep = TimeSpan.FromMinutes(1), MaxStep = TimeSpan.FromMinutes(5), DefaultStep = TimeSpan.FromMinutes(1) };
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry?range=1h&step=15s"));
        var request = TelemetryChartRequest.For(TelemetryTestData.Range("q", "Q", bounds: bounds), window, TelemetryTestData.Now, false).Request!;

        Assert.That(request.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void A_window_outside_the_entrys_bounds_is_explained_rather_than_sent()
    {
        var tight = new TelemetryQueryBounds { MaxRange = TimeSpan.FromHours(24), MaxLookback = TimeSpan.FromDays(2), MaxPoints = 100 };
        var query = TelemetryTestData.Range("q", "Q", bounds: tight);

        TelemetryChartRequest Ask(string address) =>
            TelemetryChartRequest.For(query, TelemetryWindow.FromAddress(ExplorerAddress.Parse(address)), TelemetryTestData.Now, false);

        Assert.Multiple(() =>
        {
            Assert.That(Ask("/telemetry?range=7d").Problem, Is.EqualTo("This chart covers at most 1 day. Choose a shorter range."));
            Assert.That(Ask("/telemetry?range=7d").Request, Is.Null);
            Assert.That(Ask("/telemetry?from=20251201T000000Z&to=20251201T010000Z").Problem, Does.StartWith("This chart reaches back at most 2 days."));
            Assert.That(Ask("/telemetry?range=1h&step=15s").Problem, Does.Contain("draws at most 100"));
            Assert.That(Ask("/telemetry?range=1h").Problem, Is.Null, "the automatic step keeps within the point budget");
        });
    }

    [Test]
    public void The_tree_filter_and_scope_travel_only_where_they_apply()
    {
        var window = TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry?range=1h&tree=a%2Fcrm%2Forders&scope=all"));
        var byTree = TelemetryChartRequest.For(TelemetryTestData.Range("q", "Q"), window, TelemetryTestData.Now, tenancyActive: true);
        var tenantWide = TelemetryChartRequest.For(TelemetryTestData.Instant("t", "T", treeFilter: false), window, TelemetryTestData.Now, tenancyActive: true);
        var tenancyOff = TelemetryChartRequest.For(TelemetryTestData.Range("q", "Q"), window, TelemetryTestData.Now, tenancyActive: false);

        Assert.Multiple(() =>
        {
            Assert.That(byTree.Request!.TreeId, Is.EqualTo("a/crm/orders"));
            Assert.That(byTree.Request.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(tenantWide.Request!.TreeId, Is.Null);
            Assert.That(tenantWide.Request.Range, Is.EqualTo(default(TelemetryTimeRange)), "a current reading ignores the range");
            Assert.That(tenantWide.Note, Does.Contain("not broken down by tree").And.Contain("current reading"));
            Assert.That(tenancyOff.Request!.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant), "scope applies only while tenancy is on");
        });
    }

    [Test]
    public void An_entry_that_takes_no_step_is_sent_a_window_without_one()
    {
        var query = TelemetryTestData.Range("q", "Q") with { Parameters = TelemetryQueryParameters.TimeRange };
        var request = TelemetryChartRequest.For(query, TelemetryWindow.FromAddress(ExplorerAddress.Parse("/telemetry?range=1h&step=5m")), TelemetryTestData.Now, false).Request!;

        Assert.That(request.Range.Step, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void Boards_group_the_catalogue_and_name_what_it_left_out()
    {
        var plans = TelemetryBoards.Plan(TelemetryTestData.Without("tree.write.latency_p95"), tenancyActive: false);
        var latency = TelemetryBoards.Find(plans, "latency")!;

        Assert.Multiple(() =>
        {
            Assert.That(plans.Select(plan => plan.Board.Key), Is.EqualTo(new[] { "throughput", "latency", "storage", "pressure" }));
            Assert.That(latency.Charts.Select(chart => chart.QueryId), Is.EqualTo(new[] { "tree.scan.latency_p95" }));
            Assert.That(latency.Omitted, Is.EqualTo(new[] { "Write latency (p95)" }));
            Assert.That(latency.Total, Is.EqualTo(2));
            Assert.That(latency.HasCharts, Is.True);
            Assert.That(TelemetryBoards.Find(plans, "tenant"), Is.Null, "the tenant board exists only while tenancy is on");
            Assert.That(TelemetryBoards.Find(plans, null), Is.Null);
        });
    }

    [Test]
    public void The_tenant_board_appears_with_tenancy_and_unclaimed_queries_land_on_other()
    {
        var extra = TelemetryTestData.Range("host.custom.rate", "Custom rate");
        var catalog = TelemetryTestData.CatalogOf([.. TelemetryTestData.FullCatalog().Queries, extra]);

        var plans = TelemetryBoards.Plan(catalog, tenancyActive: true);

        Assert.Multiple(() =>
        {
            Assert.That(plans.Select(plan => plan.Board.Key), Is.EqualTo(new[] { "throughput", "latency", "storage", "pressure", "tenant", "other" }));
            Assert.That(TelemetryBoards.Find(plans, "tenant")!.Charts, Has.Count.EqualTo(2));
            Assert.That(TelemetryBoards.Find(plans, "other")!.Charts, Is.EqualTo(new[] { extra }));
            Assert.That(TelemetryBoards.Curated.SelectMany(board => board.Queries).Select(query => query.QueryId), Is.Unique);
            Assert.That(TelemetryBoards.Curated.Select(board => board.Key), Is.All.Matches<string>(key => key == key.ToLowerInvariant()));
        });
    }

    [Test]
    public void Readings_are_written_in_their_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryFormat.Value(1536, "By", TelemetryMeasurementSemantic.Level), Is.EqualTo("1.5 KiB"));
            Assert.That(TelemetryFormat.Value(512, "By", TelemetryMeasurementSemantic.Level), Is.EqualTo("512 B"));
            Assert.That(TelemetryFormat.Value(0.931, "1", TelemetryMeasurementSemantic.Ratio), Is.EqualTo("93.1 %"));
            Assert.That(TelemetryFormat.Value(2, "1", TelemetryMeasurementSemantic.Level), Is.EqualTo("2"));
            Assert.That(TelemetryFormat.Value(12.25, "ms", TelemetryMeasurementSemantic.Duration), Is.EqualTo("12.3 ms").Or.EqualTo("12.2 ms"));
            Assert.That(TelemetryFormat.Value(1234, "{op}/s", TelemetryMeasurementSemantic.PerOperation), Is.EqualTo("1,234 op/s"));
            Assert.That(TelemetryFormat.Value(double.NaN, "ms", TelemetryMeasurementSemantic.Duration), Is.EqualTo(TelemetryFormat.NoReading));
            Assert.That(TelemetryFormat.Value(0, null, TelemetryMeasurementSemantic.Unspecified), Is.EqualTo("0"));
            Assert.That(TelemetryFormat.Unit("{tombstone}/s"), Is.EqualTo("tombstone/s"));
            Assert.That(TelemetryFormat.Unit("By"), Is.EqualTo("bytes"));
            Assert.That(TelemetryFormat.Unit("1"), Is.Empty);
            Assert.That(TelemetryFormat.Time(TelemetryTestData.Now.AddMinutes(65), withDate: false), Is.EqualTo("01:05"));
            Assert.That(TelemetryFormat.Time(TelemetryTestData.Now, withDate: true), Is.EqualTo("2026-01-01 00:00"));
        });
    }

    [Test]
    public void The_scope_caption_reports_what_was_applied_including_a_narrowing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryScopeCaption.Describe(TelemetryTenantScope.AcrossAllTenants()), Is.EqualTo(("Every tenant.", false)));
            Assert.That(TelemetryScopeCaption.Describe(TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.ActiveTenant)), Is.EqualTo(("Tenant acme.", false)));
            Assert.That(TelemetryScopeCaption.Describe(default), Is.EqualTo(("Your tenant.", false)));
            Assert.That(TelemetryScopeCaption.Describe(TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.AllTenants)),
                Is.EqualTo(("You asked for every tenant; the cluster answered for tenant acme only.", true)));
            Assert.That(TelemetryScopeCaption.Describe(TelemetryTenantScope.PinnedTo("acme", TelemetryTenantVisibility.SingleTenant)).Text,
                Is.EqualTo("You asked for another tenant; the cluster answered for tenant acme instead."));
        });
    }

    [Test]
    public void A_series_is_named_by_tree_then_tenant_and_never_by_a_physical_id()
    {
        var labelled = TelemetryTestData.Labelled(
            [new("physical_tree", "x/__resize/7f3a"), new("outcome", "committed"), new("tenant", "_platform_"), new("tree", "a/crm/orders")],
            TelemetryTestData.Now,
            1);

        Assert.Multiple(() =>
        {
            Assert.That(TelemetrySeriesView.Describe(labelled, 0), Is.EqualTo(("a/crm/orders / tenant platform / outcome committed", "a/crm/orders")));
            Assert.That(TelemetrySeriesView.Describe(TelemetryTestData.Series(null, TelemetryTestData.Now, 1), 2), Is.EqualTo(("Series 3", (string?)null)));
            Assert.That(TelemetrySeriesView.Describe(TelemetryTestData.Labelled([new("tenant", "")], TelemetryTestData.Now, 1), 0).Name, Is.EqualTo("tenant unattributed"));
        });
    }
}
