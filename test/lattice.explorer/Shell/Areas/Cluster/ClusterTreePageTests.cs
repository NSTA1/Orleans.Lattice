using Bunit;
using Microsoft.AspNetCore.Components.Routing;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Cluster;

/// <summary>
/// <c>/cluster/trees/{tree-path}</c>: the heading names the app and tenant that
/// own the tree, the operations bar and tabs follow the capability probe (a
/// restricted identity sees nothing it cannot use), an unknown tree is not found,
/// and a physical id is never shown.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTreePageTests : ClusterTestContext
{
    private const string TreeId = "t/acme/a/crm/orders";

    [Test]
    public void The_heading_is_the_logical_name_with_its_owning_app_and_tenant()
    {
        var cut = RenderAt("/cluster/trees/" + TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent.Trim(), Is.EqualTo(TreeId));
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-shell-mono"));
            Assert.That(cut.FindAll(".lt-cluster-owner").Select(owner => owner.TextContent), Is.EqualTo(new[] { "app: crm", "tenant: acme" }));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("Owned by app crm, tenant acme - 4 shards - 128 keys per leaf"));
        });
    }

    [Test]
    public void Every_grant_shows_every_operation_and_tab()
    {
        var cut = RenderAt("/cluster/trees/" + TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-cluster-actions a").Select(link => link.TextContent),
                Is.EqualTo(new[] { "Reshard", "Resize", "Snapshot", "Admin tools", "WAL placement", "Orphaned leaves" }));
            Assert.That(cut.FindAll(".lt-cluster-actions a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[]
            {
                "cluster/trees/t/acme/a/crm/orders/reshard",
                "cluster/trees/t/acme/a/crm/orders/resize",
                "cluster/trees/t/acme/a/crm/orders/snapshot",
                "cluster/trees/t/acme/a/crm/orders/tools",
                "cluster/wal?tree=t%2Facme%2Fa%2Fcrm%2Forders",
                "cluster/orphans?tree=t%2Facme%2Fa%2Fcrm%2Forders",
            }));
            Assert.That(cut.FindAll("[role=tab]").Select(tab => tab.TextContent), Is.EqualTo(new[] { "Summary", "Configuration", "Shards", "Storage", "Lifecycle" }));
        });
    }

    [Test]
    public void A_restricted_identity_sees_nothing_it_cannot_use()
    {
        Admin.ProbeCapabilitiesAsync(TreeId, Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));

        var cut = RenderAt("/cluster/trees/" + TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Nothing you can administer"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
            Assert.That(cut.FindAll("button"), Is.Empty);
        });
    }

    [Test]
    public void A_read_only_identity_sees_diagnostics_but_no_verb()
    {
        Granted = Grants.Read;

        var cut = RenderAt("/cluster/trees/" + TreeId);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));
        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "Lifecycle").Click();

        Assert.Multiple(() =>
        {
            Assert.That(HasButton(cut, "Delete tree..."), Is.False);
            Assert.That(HasButton(cut, "Purge now..."), Is.False);
            Assert.That(HasButton(cut, "Set alias..."), Is.False);
            Assert.That(cut.Markup, Does.Contain("Purge requires the TreeLifecycle grant and is not an app operation"));
        });
    }

    [Test]
    public void An_unknown_tree_is_not_found()
    {
        Admin.GetTreeConfigAsync("missing", Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = "missing", Exists = false });
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt("/cluster/trees/missing");

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void An_address_that_names_no_cluster_page_is_not_found()
    {
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt("/cluster/nothing");

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void The_summary_says_whether_the_name_is_aliased_without_the_physical_id()
    {
        Admin.ResolveTreeAliasAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = "orders-physical-7f3a", IsAliased = true });
        Admin.GetTreeStatsAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeStatsReport { TreeId = TreeId, TotalLiveKeys = 48210, ShardCount = 4 });
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeReshardStatus { TreeId = TreeId, InProgress = true, RequestedShardCount = 8 });
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeResizeStatus { TreeId = TreeId, CurrentMaxLeafKeys = 128, CurrentMaxInternalChildren = 64 });
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId });
        Admin.InspectShardMapAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new ShardMapInspection { TreeId = TreeId, PhysicalTreeId = "orders-physical-7f3a" });
        Admin.GetTreeConfigAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeConfigurationReport { TreeId = TreeId, Exists = true, PhysicalTreeId = "orders-physical-7f3a" });

        var cut = RenderAt("/cluster/trees/" + TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("48,210"));
            Assert.That(cut.Markup, Does.Contain("Aliased: this name resolves to another physical tree"));
            Assert.That(cut.Markup, Does.Contain("In progress, to 8 shards."));
            Assert.That(cut.Markup, Does.Contain("128 keys per leaf, 64 children per node."));
            Assert.That(cut.Markup, Does.Contain("None in progress."));
        });

        foreach (var tab in new[] { "Configuration", "Shards", "Storage", "Lifecycle" })
        {
            cut.FindAll("[role=tab]").Single(candidate => candidate.TextContent == tab).Click();
            cut.WaitUntil(() => Assert.That(cut.Markup, Does.Not.Contain("orders-physical-7f3a"), tab));
        }
    }
}
