using Bunit;
using Microsoft.AspNetCore.Components.Routing;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

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

        var cut = RenderAt("/cluster/trees/" + TreeId + "?tab=lifecycle");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));

        Assert.Multiple(() =>
        {
            Assert.That(HasButton(cut, "Delete tree..."), Is.False);
            Assert.That(HasButton(cut, "Purge now..."), Is.False);
            Assert.That(HasButton(cut, "Set alias..."), Is.False);
            Assert.That(cut.Markup, Does.Contain("Purge requires the TreeLifecycle grant and is not an app operation"));
        });
    }

    [Test]
    public void A_query_string_opens_its_tab_and_never_joins_the_tree_id()
    {
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt("/cluster/trees/factory-floor?tab=lifecycle");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent.Trim(), Is.EqualTo("factory-floor"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Lifecycle"));
        });
        Assert.Multiple(() =>
        {
            Assert.That(notFound, Is.Zero);
            Admin.Received().GetTreeConfigAsync("factory-floor", Arg.Any<CancellationToken>());
            Admin.DidNotReceive().GetTreeConfigAsync(Arg.Is<string>(id => id.Contains('?') || id.Contains("tab")), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public void An_unknown_tab_opens_the_summary()
    {
        var cut = RenderAt("/cluster/trees/" + TreeId + "?tab=nonsense");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Summary")));
    }

    [Test]
    public void Choosing_a_tab_gives_it_its_own_address_and_the_summary_has_none()
    {
        var cut = RenderAt("/cluster/trees/" + TreeId);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "Storage").Click();
        var storage = Navigation.Uri;

        var onStorage = RenderAt("/cluster/trees/" + TreeId + "?tab=storage");
        onStorage.WaitUntil(() => Assert.That(onStorage.FindAll("[role=tab]"), Has.Count.EqualTo(5)));
        onStorage.FindAll("[role=tab]").Single(tab => tab.TextContent == "Summary").Click();

        Assert.Multiple(() =>
        {
            Assert.That(storage, Is.EqualTo(Navigation.BaseUri + "cluster/trees/t/acme/a/crm/orders?tab=storage"));
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "cluster/trees/t/acme/a/crm/orders"));
        });
    }

    [Test]
    public void Under_another_tenant_a_bare_name_that_is_not_its_tree_says_so_rather_than_not_found()
    {
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Connection.ILatticeActiveTenantProvider>(new Orleans.Lattice.Explorer.Tests.Connection.FakeActiveTenantProvider("globex"));
        Admin.GetTreeConfigAsync("factory-floor", Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = "factory-floor", Exists = false });
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt("/cluster/trees/factory-floor?tab=lifecycle");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Tenant globex has no tree by this name")));
        Assert.Multiple(() =>
        {
            Assert.That(notFound, Is.Zero);
            Assert.That(cut.Find(".lt-empty a").GetAttribute("href"), Is.EqualTo("cluster/trees"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
        });
    }

    [Test]
    public void Under_another_tenant_a_qualified_name_that_does_not_exist_is_not_found()
    {
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Connection.ILatticeActiveTenantProvider>(new Orleans.Lattice.Explorer.Tests.Connection.FakeActiveTenantProvider("globex"));
        Admin.GetTreeConfigAsync("t/globex/missing", Arg.Any<CancellationToken>()).Returns(new TreeConfigurationReport { TreeId = "t/globex/missing", Exists = false });
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt("/cluster/trees/t/globex/missing");

        Assert.That(notFound, Is.EqualTo(1));
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
            Assert.That(cut.Markup, Does.Contain("In progress, to 8 physical shards."));
            Assert.That(cut.Markup, Does.Contain("128 keys per leaf, 64 children per node."));
            Assert.That(cut.Markup, Does.Contain("None in progress."));
        });

        foreach (var tab in new[] { "configuration", "shards", "storage", "lifecycle" })
        {
            var view = RenderAt("/cluster/trees/" + TreeId + "?tab=" + tab);
            view.WaitUntil(() =>
            {
                Assert.That(view.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo(char.ToUpperInvariant(tab[0]) + tab[1..]));
                Assert.That(view.Markup, Does.Not.Contain("orders-physical-7f3a"), tab);
            });
        }
    }
}
