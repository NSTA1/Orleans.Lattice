using AngleSharp.Dom;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Components;
using NSubstitute;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Areas.Telemetry;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// The Telemetry page's list of boards at <c>/telemetry</c>: the boards the caller's
/// catalogue fills, what the catalogue left out, the empty, loading and failed states,
/// the refresh command's visible control, and the compact rows.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
[SetCulture("en-US")]
public sealed class TelemetryIndexPageTests : TelemetryTestContext
{
    [Test]
    public void The_boards_are_listed_as_a_booktabs_table_of_links()
    {
        var cut = RenderPage("telemetry");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Telemetry"));
            Assert.That(cut.FindAll("tbody th a").Select(link => (link.TextContent, link.GetAttribute("href"))), Is.EqualTo(new[]
            {
                ("Throughput", "telemetry/throughput"),
                ("Latency", "telemetry/latency"),
                ("Storage", "telemetry/storage"),
                ("Pressure", "telemetry/pressure"),
            }));
            Assert.That(cut.FindAll("tbody tr")[0].TextContent, Does.Contain("4 charts").And.Contain("Reads, writes and atomic writes"));
            Assert.That(cut.FindAll(".lt-telemetry-note"), Is.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("card"));
        });
    }

    [Test]
    public void What_the_allow_list_left_out_is_named_in_a_note_not_an_error()
    {
        Telemetry.Catalog = TelemetryTestData.Without("tree.write.latency_p95", "tree.scan.latency_p95", "tree.cache.hit_ratio");

        var cut = RenderPage("telemetry");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody th a").Select(link => link.TextContent), Is.EqualTo(new[] { "Throughput", "Storage", "Pressure" }),
                "a board with nothing to draw is not listed");
            Assert.That(cut.FindAll("tbody tr")[2].TextContent, Does.Contain("2 of 3 charts"));
            Assert.That(cut.Find(".lt-telemetry-note").TextContent,
                Is.EqualTo("Not shown: Write latency (p95), Scan latency (p95), Cache hit ratio. Your grants or the cluster's metric allow-list do not admit them."));
            Assert.That(cut.Find(".lt-telemetry-note").GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
        });
    }

    [Test]
    public void An_empty_catalogue_explains_both_reasons_the_facade_will_not_tell_apart()
    {
        Telemetry.Catalog = TelemetryQueryCatalog.Empty;

        var cut = RenderPage("telemetry");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No metrics to show"));
            Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("no metrics backend is configured").And.Contain("allow-list admit none"));
            Assert.That(cut.FindAll("table"), Is.Empty);
        });
    }

    [Test]
    public void The_catalogue_loads_behind_a_skeleton()
    {
        Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();

        var cut = RenderPage("telemetry");
        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));

        cut.InvokeAsync(() => Telemetry.PendingCatalog.SetResult(TelemetryTestData.FullCatalog()));

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(4)));
    }

    [Test]
    public void A_catalogue_that_did_not_arrive_can_be_asked_for_again()
    {
        Telemetry.CatalogFailure = new TelemetryBackendException(string.Empty, "down");

        var cut = RenderPage("telemetry");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Telemetry did not answer")));

        Telemetry.CatalogFailure = null;
        cut.Find(".lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(4)));
        Assert.That(Telemetry.CatalogReads, Is.EqualTo(2));
    }

    [Test]
    public void Refresh_is_the_palette_commands_visible_control_and_re_reads_the_catalogue()
    {
        var area = Services.GetServices<IExplorerArea>().OfType<TelemetryArea>().Single();
        var command = area.Commands.Single();
        var cut = RenderPage("telemetry");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(4)));

        ExplorerCommandControls.AssertVisibleControl(cut, command);

        Telemetry.Catalog = TelemetryTestData.Without("tree.read.operation_rate");
        cut.InvokeAsync(() => command.InvokeAsync!.Invoke(CancellationToken.None).AsTask());

        cut.WaitUntil(() =>
        {
            Assert.That(Telemetry.CatalogReads, Is.EqualTo(2));
            Assert.That(cut.FindAll("tbody tr")[0].TextContent, Does.Contain("3 of 4 charts"));
        });

        cut.Find($"[{ExplorerCommand.ControlAttribute}=\"{command.Id}\"]").Click();
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.EqualTo(3)));
    }

    [Test]
    public void With_tenancy_on_the_list_is_the_tenants_and_includes_its_board()
    {
        UseTenancy("acme");

        var cut = RenderPage("t/acme/telemetry");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("For tenant acme."));
            Assert.That(cut.FindAll("tbody th a").Select(link => link.GetAttribute("href")), Does.Contain("t/acme/telemetry/tenant"));
            Assert.That(cut.FindAll("tbody th a").Select(link => link.GetAttribute("href")), Is.All.StartsWith("t/acme/telemetry/"));
        });
    }

    [Test]
    public void With_tenancy_off_there_is_no_tenant_wording_or_board()
    {
        var cut = RenderPage("telemetry");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Not.Contain("tenant"));
            Assert.That(cut.FindAll("tbody th a").Select(link => link.TextContent), Does.Not.Contain("Tenant"));
        });
    }

    [Test]
    public void At_compact_each_board_is_a_two_line_row_opening_a_sheet_with_its_link()
    {
        var cut = RenderPage("telemetry", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(4)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo("Throughput"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("4 charts"));
        });

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog a.lt-telemetry-action").GetAttribute("href"), Is.EqualTo("telemetry/throughput")));
    }
}
