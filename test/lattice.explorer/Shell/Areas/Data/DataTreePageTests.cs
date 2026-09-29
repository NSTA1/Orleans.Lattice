using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// The tree workspace at <c>/data/{tree-path}</c>: the heading and summary, the
/// tab row driven by <c>?tab=</c>, not-found for a tree the caller cannot reach,
/// and tenancy, where the tenant-composed id never reaches the page.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTreePageTests : DataTestContext
{
    [Test]
    public void The_workspace_heads_with_the_logical_id_its_app_and_a_summary()
    {
        Client.WithTree("a/crm/orders", keys: 3, shards: 64);
        Client.Metrics["a/crm/orders"] = new TreeMetrics { TreeId = "a/crm/orders", ShardCount = 64, LiveKeys = 48_210 };

        var cut = RenderAt("data/a/crm/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-shell-mono"));
            Assert.That(cut.FindAll(".lt-data-heading .lt-pill").Select(pill => pill.TextContent.Trim()), Is.EqualTo(new[] { "App tree", "app: crm" }));
            var meta = cut.Find(".lt-data-meta");
            Assert.That(meta.TextContent, Does.Contain("Owned by app crm").And.Contain("64 shards").And.Contain("48,210 live keys"));
            Assert.That(meta.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("apps/crm"));
            Assert.That(cut.FindAll("[role=tab]").Select(tab => tab.TextContent), Is.EqualTo(new[] { "Keys", "History", "Metrics", "Dead letters", "Tag indexes", "Views" }));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Keys"));
        });
    }

    [Test]
    public void Choosing_a_tab_puts_it_in_the_address_and_keeps_the_key()
    {
        Client.WithTree("orders", keys: 3);
        var cut = RenderAt("data/orders?key=key%2F0001");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-entry"), Has.Count.EqualTo(1)));

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "History").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentRelative, Is.EqualTo("/data/orders?key=key%2F0001&tab=history"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("History"));
            Assert.That(cut.Find(".lt-data-section-title").TextContent, Does.StartWith("History of"));
        });
    }

    [Test]
    public void A_tree_the_caller_cannot_reach_is_not_found()
    {
        Client.WithTree("orders");

        var cut = RenderAt("data/secret/ledger");

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address")));
    }

    [Test]
    public void With_tenancy_on_the_workspace_reads_the_composed_id_but_shows_only_the_logical_one()
    {
        UseDataTenancy("acme");
        Client.WithTree("t/acme/a/crm/orders", keys: 2);

        var cut = RenderAt("t/acme/data/a/crm/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(Rows(cut), Has.Count.EqualTo(2));
        });
        Assert.Multiple(() =>
        {
            Assert.That(Client.Calls, Has.Some.StartsWith("ScanEntriesAsync"));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/a/crm"), "the tenant-composed id never reaches the page");
            Assert.That(Rows(cut)[0].QuerySelector("a")!.GetAttribute("href"), Does.StartWith("t/acme/data/a/crm/orders?key="));
        });
    }

    [Test]
    public void A_catalogue_failure_on_the_workspace_says_so_and_retries()
    {
        Client.Fault = call => call == nameof(Orleans.Lattice.Explorer.Core.Connection.ILatticeStateClient.ListTreesAsync)
            ? new InvalidOperationException("secret detail")
            : null;
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("This tree did not load")));
        Assert.That(cut.Markup, Does.Not.Contain("secret detail"));

        Client.Fault = null;
        Client.WithTree("orders", keys: 1);
        Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("orders")));
    }

    [Test]
    public void A_view_workspace_names_its_source_tree()
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Entries["view-by-status"] = new(StringComparer.Ordinal);

        var cut = RenderAt("data/view-by-status");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("by-status"));
            Assert.That(cut.Find(".lt-data-meta").TextContent, Does.Contain("View of"));
            Assert.That(cut.Find(".lt-data-meta a").GetAttribute("href"), Is.EqualTo("data/orders"));
        });
    }
}
