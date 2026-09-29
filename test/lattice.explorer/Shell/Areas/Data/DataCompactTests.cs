using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// Below the small breakpoint every Data table is a list of two-line rows with a
/// detail sheet - none falls back to a scroll frame - and the toolbars keep the
/// stacking toolbar class.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataCompactTests : DataTestContext
{
    [Test]
    public void The_metrics_figures_are_compact_lists()
    {
        Client.WithTree("orders");
        Client.Metrics["orders"] = new TreeMetrics
        {
            TreeId = "orders",
            ShardCount = 1,
            LiveKeys = 5,
            ShardHotness = [new ShardHotness { ShardIndex = 0, OpsPerSecond = 1, LiveKeys = 5 }],
        };

        var cut = RenderAt("data/orders?tab=metrics", compact: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.FindAll(".lt-table-list"), Has.Count.EqualTo(2));
            Assert.That(cut.FindAll(".lt-compact-row__primary").Select(line => line.TextContent), Does.Contain("Live keys").And.Contain("0"));
            Assert.That(cut.FindAll(".lt-toolbar"), Is.Not.Empty);
        });
    }

    [Test]
    public void The_tag_index_list_and_its_members_are_compact_lists()
    {
        Client.WithTree("orders", keys: 2);
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders"];
        Client.Members[("by-region", "eu")] = [new TagMember { TreeId = "orders", Key = "key/0001" }];
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagIndexStatus { IndexName = "by-region", TreeId = "tag-by-region", CoveredTrees = ["orders"], ReconcileIdle = true });

        var cut = RenderAt("data/orders?tab=tag-indexes&index=by-region&tag=eu", compact: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.FindAll(".lt-table-list"), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("1 covered trees"));
        });

        cut.FindAll(".lt-table-list__open")[0].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog a.lt-btn").GetAttribute("href"), Is.EqualTo("data/orders?tab=tag-indexes&index=by-region")));
    }

    [Test]
    public void The_keys_toolbar_stacks_and_every_control_keeps_its_label()
    {
        Client.WithTree("orders", keys: 1);

        var cut = RenderAt("data/orders", compact: true);

        cut.WaitUntil(() =>
        {
            var toolbar = cut.Find(".lt-toolbar");
            Assert.That(toolbar.QuerySelectorAll("label").Select(label => label.TextContent.Trim()), Is.SupersetOf(new[] { "Key prefix", "Scan", "Page size" }));
            Assert.That(toolbar.QuerySelector("button[role=switch]"), Is.Not.Null);
        });
    }
}
