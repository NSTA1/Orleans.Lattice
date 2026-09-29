using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// The Data directory at <c>/data</c>: logical names only, app ownership, views,
/// filtering, a large virtualised list, tenancy on and off, the compact form, and
/// its loading, empty and error states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataDirectoryPageTests : DataTestContext
{
    [Test]
    public void The_directory_lists_trees_and_views_by_logical_name_with_their_app_and_source()
    {
        Client.WithTree("a/crm/orders", shards: 64).WithTree("customers");
        Client.Views.Add(new ViewStateSummary { ViewName = "orders-by-status", SourceTreeId = "a/crm/orders", IsAggregation = true });

        var cut = RenderAt("data");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Data"));
            var rows = cut.FindAll("tbody tr.lt-table__row");
            Assert.That(rows, Has.Count.EqualTo(3));
            Assert.That(rows[0].QuerySelector("th a")!.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(rows[0].QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("data/a/crm/orders"));
            Assert.That(rows[0].TextContent, Does.Contain("App tree").And.Contain("64").And.Contain("Active"));
            Assert.That(cut.Find("a[href='apps/crm']").TextContent, Is.EqualTo("crm"));
            var view = rows.Single(row => row.TextContent.Contains("orders-by-status", StringComparison.Ordinal));
            Assert.That(view.TextContent, Does.Contain("Aggregation view"));
            Assert.That(view.QuerySelectorAll("a").Select(link => link.GetAttribute("href")), Does.Contain("data/a/crm/orders"));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("3 trees and views"));
        });
    }

    [Test]
    public void Physical_system_and_restore_shadow_trees_never_appear()
    {
        Client.WithTree("orders");
        Client.Trees.Add(FakeStateClient.Tree("_lattice_trees"));
        Client.Trees.Add(FakeStateClient.Tree("sys-app-registry"));
        Client.Trees.Add(FakeStateClient.Tree("orders-restore-7f3a", restoreShadowOf: "orders"));

        var cut = RenderAt("data");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1));
            Assert.That(cut.Markup, Does.Not.Contain("_lattice_trees").And.Not.Contain("sys-app").And.Not.Contain("restore-7f3a"));
        });
    }

    [Test]
    public void With_tenancy_on_the_tenant_prefix_is_stripped_and_links_are_tenant_rooted()
    {
        UseDataTenancy("acme");
        Client.WithTree("t/acme/a/crm/orders").WithTree("t/globex/secret");

        var cut = RenderAt("t/acme/data");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("tenant acme"));
            var link = cut.Find("tbody th a");
            Assert.That(link.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(link.GetAttribute("href"), Is.EqualTo("t/acme/data/a/crm/orders"));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/a/crm").And.Not.Contain("globex"));
        });
    }

    [Test]
    public void The_filter_narrows_by_name_or_app_and_the_kind_buttons_by_kind()
    {
        Client.WithTree("a/crm/orders").WithTree("a/billing/invoices").WithTree("customers");
        Client.Views.Add(new ViewStateSummary { ViewName = "crm-summary", SourceTreeId = "a/crm/orders" });
        var cut = RenderAt("data");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(4)));

        cut.Find("input[type=search]").Input("crm");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("2 of 4 match"));
        });

        cut.FindAll(".lt-data-segmented button").Single(button => button.TextContent == "Views").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-data-segmented button").Single(button => button.TextContent == "Views").GetAttribute("aria-pressed"), Is.EqualTo("true"));
        });

        cut.Find("input[type=search]").Input("nothing-matches");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent, Does.Contain("Nothing matches")));
    }

    [Test]
    public void The_filter_starts_from_the_address()
    {
        Client.WithTree("orders").WithTree("customers");

        var cut = RenderAt("data?filter=cust");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Thousands_of_trees_are_virtualised_and_read_across_catalogue_pages()
    {
        for (var i = 0; i < 5000; i++)
        {
            Client.Trees.Add(FakeStateClient.Tree($"tree-{i:D5}"));
        }

        var cut = RenderAt("data");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-toolbar__status").TextContent, Is.EqualTo("5,000 trees and views"));
            var rendered = cut.FindAll("tbody tr.lt-table__row").Count;
            Assert.That(rendered, Is.GreaterThan(0).And.LessThan(200), "only the rows in view are rendered");
        });
        Assert.That(Client.Calls.Count(call => call == nameof(ILatticeStateClient.ListTreesAsync)), Is.EqualTo(10), "5,000 trees are ten catalogue pages of 500");
    }

    [Test]
    public void The_directory_shows_a_skeleton_while_loading_and_an_empty_state_with_no_trees()
    {
        Client.CatalogGate = new TaskCompletionSource();
        var cut = RenderAt("data");
        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));

        Client.CatalogGate.SetResult();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No trees yet")));
    }

    [Test]
    public void A_catalogue_failure_says_so_in_a_fixed_sentence_and_retries()
    {
        Client.Fault = call => call == nameof(ILatticeStateClient.ListTreesAsync) ? new InvalidOperationException("t/acme/orders exploded") : null;
        var cut = RenderAt("data");
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The directory did not load"));
            Assert.That(cut.Markup, Does.Not.Contain("exploded"));
        });

        Client.Fault = null;
        Client.WithTree("orders");
        cut.FindAll(".lt-empty button").Single().Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Compact_rows_show_the_name_and_a_summary_and_open_a_detail_sheet_with_an_open_link()
    {
        Client.WithTree("a/crm/orders", shards: 64);

        var cut = RenderAt("data", compact: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("App tree - 64 shards - app crm"));
        });

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog");
            Assert.That(sheet.QuerySelector("h2")!.TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(sheet.QuerySelectorAll("a.lt-btn").Single().GetAttribute("href"), Is.EqualTo("data/a/crm/orders"));
        });
    }
}
