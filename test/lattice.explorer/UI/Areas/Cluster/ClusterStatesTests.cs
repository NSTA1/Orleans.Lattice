using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// Cross-cutting states: loading skeletons before a read answers, tenancy on
/// (the area stays cluster-wide and names the owning tenant), untrusted names
/// rendered as text only, and the compact form of the findings table.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterStatesTests : ClusterTestContext
{
    [Test]
    public void Before_a_read_answers_each_page_shows_a_skeleton()
    {
        var trees = new TaskCompletionSource<TreeCatalogPage>();
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>()).Returns(trees.Task);
        var probe = new TaskCompletionSource<LatticeTreeAdminCapabilities>();
        Admin.ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>()).Returns(probe.Task);

        var list = RenderAt("/cluster/trees");
        var tree = RenderAt("/cluster/trees/orders");

        Assert.Multiple(() =>
        {
            Assert.That(list.Find(".lt-skeleton").GetAttribute("aria-label") ?? list.Find(".lt-skeleton").TextContent, Does.Contain("Loading trees"));
            Assert.That(tree.Find(".lt-skeleton").GetAttribute("aria-label") ?? tree.Find(".lt-skeleton").TextContent, Does.Contain("Loading the tree"));
        });

        trees.SetResult(new TreeCatalogPage { Entries = [Tree("orders")] });
        list.WaitUntil(() => Assert.That(list.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void With_tenancy_on_the_area_stays_cluster_wide_and_names_each_trees_tenant()
    {
        UseTenancy("acme");
        UseTrees(Tree("t/acme/orders"), Tree("t/globex/a/crm/invoices"));

        var cut = RenderAt("/cluster/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr a").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "cluster/trees/t/acme/orders", "cluster/trees/t/globex/a/crm/invoices" }), "cluster addresses never carry a tenant root");
            Assert.That(cut.FindAll("tbody tr")[0].TextContent, Does.Contain("tenant acme"));
            Assert.That(cut.FindAll("tbody tr")[1].TextContent, Does.Contain("app crm, tenant globex"));
        });
    }

    [Test]
    public void With_tenancy_on_the_overview_stops_are_not_tenant_rooted()
    {
        UseTenancy("acme");

        var cut = RenderAt("/cluster");

        Assert.That(cut.FindAll(".lt-cluster-stops__link").Select(link => link.GetAttribute("href")),
            Is.EqualTo(new[] { "cluster/trees", "cluster/wal", "cluster/orphans" }));
    }

    [Test]
    public void Names_from_the_cluster_and_apps_render_as_text_only()
    {
        const string Hostile = "a/<img src=x onerror=alert(1)>/orders";
        UseTrees(Tree(Hostile));

        var list = RenderAt("/cluster/trees");
        var page = RenderAt(Orleans.Lattice.Explorer.UI.Areas.Cluster.ClusterAddresses.Tree(Hostile).Format());

        list.WaitUntil(() => Assert.That(list.FindAll("tbody tr"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(list.FindAll("img"), Is.Empty);
            Assert.That(page.FindAll("img"), Is.Empty);
            Assert.That(page.Find(".lt-cluster-owner").TextContent, Is.EqualTo("app: <img src=x onerror=alert(1)>"));
        });
    }

    [Test]
    public void Orphan_findings_read_as_compact_rows()
    {
        Admin.AuditOrphanedLeavesAsync("orders", null, Arg.Any<CancellationToken>()).Returns(new TreeOrphanedLeafReport
        {
            TreeId = "orders",
            Findings = [new TreeOrphanedLeafFinding { LeafId = "leaf-9", ShardIndex = 3, KeyCount = 12, Disposition = TreeOrphanedLeafDisposition.Repairable }],
        });
        Tracked.Script(
            Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationKinds.OrphanedLeavesAudit,
            Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript.Status(
                Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationKinds.OrphanedLeavesAudit,
                Orleans.Lattice.Api.Operations.LatticeOperationState.Succeeded,
                "Completed",
                result: new Dictionary<string, string> { [Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationResultKeys.OrphanedLeaves] = "1", [Orleans.Lattice.Api.TreeAdmin.TreeAdminOperationResultKeys.Repairable] = "1" }));
        var cut = RenderAt("/cluster/orphans?tree=orders", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Show each leaf"), Is.True));
        Button(cut, "Show each leaf").Click();

        cut.WaitUntil(() =>
        {
            var row = cut.Find(".lt-table-list__row");
            Assert.That(row.QuerySelector(".lt-compact-row__primary")!.TextContent, Is.EqualTo("leaf-9"));
            Assert.That(row.TextContent, Does.Contain("shard 3, 12 keys").And.Contain("Repairable"));
        });
    }
}
