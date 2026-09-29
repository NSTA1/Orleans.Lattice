using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Microsoft.AspNetCore.Components;
using NSubstitute;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Areas.Telemetry;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Telemetry;

/// <summary>
/// One Telemetry board: its charts, the board navigation, every window parameter in
/// the address (range, absolute window, step, tree, scope and view) and the note the
/// board shows for what it may not draw - never an error page.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
[SetCulture("en-US")]
public sealed class TelemetryBoardPageTests : TelemetryTestContext
{
    [Test]
    public void A_board_draws_each_of_its_charts_under_one_heading_and_marks_itself_in_the_board_row()
    {
        var cut = RenderPage("telemetry/latency");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Latency"));
            Assert.That(cut.FindAll(".lt-telemetry-chart h2").Select(heading => heading.TextContent), Is.EqualTo(new[] { "Write latency p95", "Scan latency p95" }));
            Assert.That(cut.Find(".lt-telemetry-boards").GetAttribute("aria-label"), Is.EqualTo("Boards"));
            Assert.That(cut.Find(".lt-telemetry-boards__link[aria-current=page]").TextContent, Is.EqualTo("Latency"));
            Assert.That(cut.FindAll(".lt-telemetry-boards__link").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "telemetry/throughput", "telemetry/latency", "telemetry/storage", "telemetry/pressure" }));
            Assert.That(Telemetry.Requests.Select(request => request.QueryId), Is.EquivalentTo(new[] { "tree.write.latency_p95", "tree.scan.latency_p95" }));
        });
    }

    [Test]
    public void Without_a_range_in_the_address_each_chart_asks_for_its_default_window()
    {
        var cut = RenderPage("telemetry/latency");

        cut.WaitUntil(() =>
        {
            Assert.That(Telemetry.Requests, Has.Count.EqualTo(2));
            Assert.That(Telemetry.Requests.Select(request => request.Range), Is.All.EqualTo(default(TelemetryTimeRange)));
            Assert.That(cut.Find(".lt-telemetry-choice__option[aria-current=true]").TextContent, Is.EqualTo("Default"));
            Assert.That(cut.FindAll(".lt-select select").Select(select => select.GetAttribute("aria-label") ?? string.Empty), Is.Empty.Or.Not.Contain("Step"));
        });
    }

    [Test]
    public void The_range_is_in_the_address_so_every_chart_state_is_a_link()
    {
        var cut = RenderPage("telemetry/latency?range=6h&tree=a%2Fcrm%2Forders");

        cut.WaitUntil(() =>
        {
            var request = Telemetry.Requests.First();
            Assert.That(request.Range.StartUtc, Is.EqualTo(TelemetryTestData.Now.AddHours(-6)));
            Assert.That(request.Range.EndUtc, Is.EqualTo(TelemetryTestData.Now));
            Assert.That(cut.Find(".lt-telemetry-choice__option[aria-current=true]").TextContent, Is.EqualTo("6 h"));
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Time range'] a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[]
            {
                "telemetry/latency?tree=a%2Fcrm%2Forders",
                "telemetry/latency?range=15m&tree=a%2Fcrm%2Forders",
                "telemetry/latency?range=1h&tree=a%2Fcrm%2Forders",
                "telemetry/latency?range=6h&tree=a%2Fcrm%2Forders",
                "telemetry/latency?range=1d&tree=a%2Fcrm%2Forders",
                "telemetry/latency?range=7d&tree=a%2Fcrm%2Forders",
            }));
        });
    }

    [Test]
    public void A_relative_window_can_be_pinned_to_an_absolute_one_and_read_back()
    {
        var relative = RenderPage("telemetry/latency?range=1h");
        relative.WaitUntil(() => Assert.That(relative.Find("a.lt-telemetry-action").TextContent, Is.EqualTo("Pin this window")));
        var pinned = relative.Find("a.lt-telemetry-action").GetAttribute("href")!;
        Assert.That(pinned, Does.Contain("from=20251231T230000Z").And.Contain("to=20260101T000000Z").And.Not.Contain("range="));

        Telemetry.Requests.Clear();
        var absolute = RenderPage(pinned);

        absolute.WaitUntil(() =>
        {
            Assert.That(absolute.Find(".lt-telemetry-filter").TextContent, Does.Contain("From 2025-12-31 23:00").And.Contain("to 2026-01-01 00:00"));
            Assert.That(Telemetry.Requests.First().Range.StartUtc, Is.EqualTo(TelemetryTestData.Now.AddHours(-1)));
            Assert.That(absolute.FindAll(".lt-telemetry-choice__option[aria-current=true]").Select(option => option.TextContent), Is.EqualTo(new[] { "Charts" }),
                "no relative range is current while an absolute window is pinned");
        });
    }

    [Test]
    public void A_step_is_offered_once_a_range_is_chosen_and_travels_in_the_address()
    {
        var cut = RenderPage("telemetry/latency?range=1h&step=5m");

        cut.WaitUntil(() =>
        {
            Assert.That(Telemetry.Requests.First().Range.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(cut.Find("select").GetAttribute("value") ?? cut.Find("select option[selected]").GetAttribute("value"), Is.EqualTo("5m"));
        });

        cut.Find("select").Change("15m");

        Assert.That(Navigation.Uri, Does.Contain("step=15m").And.Contain("range=1h"));
    }

    [Test]
    public void An_address_the_area_cannot_read_still_lands_on_the_board_with_a_notice()
    {
        var cut = RenderPage("telemetry/latency?range=soon");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-telemetry-notes").TextContent, Does.Contain("the time range in a form the Explorer cannot read"));
            Assert.That(cut.FindAll(".lt-telemetry-chart"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void A_tree_filter_narrows_every_chart_that_is_broken_down_by_tree_and_links_to_Data()
    {
        var cut = RenderPage("telemetry/storage?tree=a%2Fcrm%2Forders");

        cut.WaitUntil(() =>
        {
            Assert.That(Telemetry.Requests.Select(request => request.TreeId), Is.All.EqualTo("a/crm/orders"));
            var filter = cut.Find(".lt-telemetry-filter");
            Assert.That(filter.TextContent, Does.Contain("Narrowed to tree a/crm/orders."));
            Assert.That(filter.QuerySelectorAll("a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "telemetry/storage", "data/a/crm/orders" }));
        });
    }

    [Test]
    public void A_chart_the_cluster_refuses_is_omitted_with_a_note_never_an_error_page()
    {
        Telemetry.Fail("tree.scan.latency_p95", new UnauthorizedAccessException("denied"));

        var cut = RenderPage("telemetry/latency");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-telemetry-chart h2").Select(heading => heading.TextContent), Is.EqualTo(new[] { "Write latency p95" }));
            Assert.That(cut.Find(".lt-telemetry-notes").TextContent,
                Is.EqualTo("Not shown: Scan latency p95. Your grants or the cluster's metric allow-list do not admit it."));
            Assert.That(cut.Find(".lt-telemetry-notes").GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [Test]
    public void A_board_whose_charts_the_allow_list_all_left_out_explains_itself()
    {
        Telemetry.Catalog = TelemetryTestData.Without("tree.write.latency_p95", "tree.scan.latency_p95");

        var cut = RenderPage("telemetry/latency");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Latency"));
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Nothing on this board is available to you"));
            Assert.That(cut.Find(".lt-telemetry-notes").TextContent, Does.Contain("Write latency (p95), Scan latency (p95)"));
            Assert.That(Telemetry.Requests, Is.Empty);
        });
    }

    [Test]
    public void The_table_view_is_in_the_address_and_draws_every_chart_as_a_table()
    {
        Telemetry.Answer("tree.write.latency_p95", request => TelemetryTestData.Response(request, default,
            TelemetryTestData.Series("a/crm/orders", TelemetryTestData.Now, 1, 2)));

        var cut = RenderPage("telemetry/latency?view=table");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("svg.lt-telemetry-svg"), Is.Empty);
            Assert.That(cut.FindAll("table"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-telemetry-choice[aria-label=View] [aria-current=true]").TextContent, Is.EqualTo("Tables"));
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label=View] a").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "telemetry/latency", "telemetry/latency?view=table" }));
        });
    }

    [Test]
    public void An_unknown_board_or_a_deeper_path_is_not_found()
    {
        var notFound = 0;
        Services.GetRequiredService<NavigationManager>().OnNotFound += (_, _) => notFound++;

        RenderPage("telemetry/nonesuch");
        RenderPage("telemetry/latency/deeper");
        RenderPage("telemetry/tenant");

        Assert.That(notFound, Is.EqualTo(3), "the tenant board does not exist while tenancy is off");
    }

    [Test]
    public void At_compact_the_boards_and_the_range_become_selects_that_navigate()
    {
        var cut = RenderPage("telemetry/latency?range=1h", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll("select"), Has.Count.GreaterThanOrEqualTo(3)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-telemetry-boards"), Is.Empty);
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Time range']"), Is.Empty);
            Assert.That(cut.FindAll(".lt-field__label").Select(label => label.TextContent), Is.SupersetOf(new[] { "Board", "Range", "Step" }));
        });

        cut.FindAll("select")[1].Change("1d");
        Assert.That(Navigation.Uri, Does.EndWith("telemetry/latency?range=1d"));

        var boards = RenderPage("telemetry/latency?range=1h", LtBreakpoint.Compact);
        boards.WaitUntil(() => Assert.That(boards.FindAll("select"), Has.Count.GreaterThanOrEqualTo(3)));
        boards.FindAll("select")[0].Change("storage");
        Assert.That(Navigation.Uri, Does.EndWith("telemetry/storage?range=1h"));
    }

    [Test]
    public void With_tenancy_on_an_operator_can_widen_to_every_tenant_and_a_narrowing_is_noted()
    {
        UseTenancy("acme");
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        Telemetry.Answer("tree.write.latency_p95", request => TelemetryTestData.Response(request,
            TelemetryTenantScope.PinnedTo("acme", request.RequestedVisibility)));

        var own = RenderPage("t/acme/telemetry/latency");
        own.WaitUntil(() =>
        {
            Assert.That(own.Find(".lt-shell-page-lede").TextContent, Does.Contain("Tenant acme."));
            Assert.That(own.FindAll(".lt-telemetry-choice[aria-label='Tenant scope'] a").Select(link => (link.TextContent, link.GetAttribute("href"))), Is.EqualTo(new[]
            {
                ("This tenant", "t/acme/telemetry/latency"),
                ("All tenants", "t/acme/telemetry/latency?scope=all"),
            }));
            Assert.That(Telemetry.Requests.Select(request => request.RequestedVisibility), Is.All.EqualTo(TelemetryTenantVisibility.ActiveTenant));
        });

        Telemetry.Requests.Clear();
        var wide = RenderPage("t/acme/telemetry/latency?scope=all");
        wide.WaitUntil(() =>
        {
            Assert.That(Telemetry.Requests.Select(request => request.RequestedVisibility), Is.All.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(wide.Find(".lt-telemetry-notes").TextContent, Does.Contain("You asked for every tenant; the cluster answered for tenant acme only."));
        });
    }

    [Test]
    public void A_tenant_who_is_not_an_operator_is_offered_no_wider_scope()
    {
        UseTenancy("acme");

        var cut = RenderPage("t/acme/telemetry/tenant");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Tenant"));
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Tenant scope']"), Is.Empty);
            Assert.That(cut.FindAll(".lt-telemetry-chart"), Has.Count.EqualTo(2));
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Time range']"), Is.Empty, "the tenant board holds only current readings");
        });
    }

    [Test]
    public void Moving_to_another_board_keeps_the_window()
    {
        var cut = RenderPage("telemetry/latency?range=1d&view=table");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-telemetry-boards__link").Select(link => link.GetAttribute("href")),
            Does.Contain("telemetry/storage?range=1d&view=table")));
    }
}
