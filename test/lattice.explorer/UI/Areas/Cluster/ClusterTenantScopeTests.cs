using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// Issue #4025: a tenant-rooted Cluster address (<c>/t/{tenant}/cluster</c>)
/// shows only that tenant's own trees and storage, and never names another
/// tenant's tree. Two tenants' trees, the default tenant's bare trees and an app
/// tree are seeded together.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterTenantScopeTests : ClusterTestContext
{
    private void SeedEstate() => UseTrees(
        Tree("a/crm/orders"),
        Tree("invoices"),
        Tree("t/acme/orders"),
        Tree("t/acme/stock"),
        Tree("t/globex/orders"));

    private static string[] Listed(IRenderedComponent<Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterPage> cut) =>
        [.. cut.FindAll("tbody tr th a, tbody tr td a").Select(link => link.TextContent).Where(text => text.Length > 0).Distinct()];

    [Test]
    public void At_a_tenant_address_the_tree_list_shows_only_that_tenants_trees()
    {
        SeedEstate();

        var cut = RenderAt("/t/acme/cluster/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Is.EquivalentTo(new[] { "t/acme/orders", "t/acme/stock" }));
            Assert.That(cut.Find("[data-lt-cluster-count]").TextContent, Does.StartWith("2 trees of tenant acme."));
        });
    }

    [Test]
    public void At_the_default_tenants_address_no_other_tenants_tree_is_listed()
    {
        SeedEstate();

        var cut = RenderAt("/t/default/cluster/trees");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Is.EquivalentTo(new[] { "a/crm/orders", "invoices" }));
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/").And.Not.Contain("t/globex/"));
        });
    }

    [Test]
    public void The_cluster_wide_address_still_lists_every_tree()
    {
        SeedEstate();

        var cut = RenderAt("/cluster/trees");

        cut.WaitUntil(() => Assert.That(Listed(cut), Has.Length.EqualTo(5)));
    }

    [TestCase("/t/acme/cluster/trees/t/globex/orders")]
    [TestCase("/t/default/cluster/trees/t/acme/orders")]
    [TestCase("/t/acme/cluster/trees/t/globex/orders/tools")]
    public void A_tenant_address_for_another_tenants_tree_is_not_found_and_never_read(string address)
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt(address);

        Assert.Multiple(() =>
        {
            Assert.That(notFound, Is.EqualTo(1));
            Assert.That(Admin.ReceivedCalls().Where(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetTreeConfigAsync)), Is.Empty);
        });
    }

    [Test]
    public void A_tenant_address_for_its_own_tree_renders_it()
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt("/t/acme/cluster/trees/t/acme/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(notFound, Is.Zero);
            Assert.That(cut.Markup, Does.Contain("t/acme/orders"));
        });
    }

    [Test]
    public void At_a_tenant_address_the_overview_reports_only_its_trees_storage_and_leaves_the_regions_to_the_cluster_page()
    {
        SeedEstate();
        Admin.GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary
            {
                TreeCount = 12,
                TotalBytes = 1 << 20,
                Trees =
                [
                    new TreeStorageUsageSnapshot { TreeId = "t/acme/orders", TotalBytes = 1024 },
                    new TreeStorageUsageSnapshot { TreeId = "t/acme/stock", TotalBytes = 1024 },
                    new TreeStorageUsageSnapshot { TreeId = "t/globex/orders", TotalBytes = 4096 },
                    new TreeStorageUsageSnapshot { TreeId = "invoices", TotalBytes = 8192 },
                ],
            });

        var cut = RenderAt("/t/acme/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Trees of tenant acme"));
            Assert.That(cut.Markup, Does.Contain("2.0 KiB"));
            Assert.That(cut.Markup, Does.Not.Contain("1.0 MiB"));
            Assert.That(cut.FindAll("#lt-cluster-regions-heading"), Is.Empty);
            Assert.That(cut.Find("[data-lt-cluster-wide] a").GetAttribute("href"), Is.EqualTo("cluster"));
        });
    }

    [Test]
    public async Task Completions_from_a_tenant_address_offer_only_its_trees_rooted_at_it()
    {
        SeedEstate();
        var source = new ClusterCompletionSource(Services.GetRequiredService<ClusterTreeCatalog>());

        var completions = await source.CompleteAsync(
            new AddressQuery("orders", AddressQueryMode.Search, ExplorerAddress.Home.WithTenant("acme")),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "t/acme/orders" }));
            Assert.That(completions.Single().Target.Tenant, Is.EqualTo("acme"));
        });
    }

    [TestCase("t/acme/orders", "acme", true)]
    [TestCase("orders", "acme", true)]
    [TestCase("t/globex/orders", "acme", false)]
    [TestCase("orders", "default", true)]
    [TestCase("t/acme/orders", "default", false)]
    [TestCase("sys-tenant-registry", "acme", false)]
    [TestCase("t/globex/orders", null, true)]
    public void Names_admits_the_tenants_own_trees_and_bare_names_it_reads_as_its_own(string treeId, string? scope, bool named)
    {
        Assert.That(ClusterTreeCatalog.Names(scope, treeId), Is.EqualTo(named));
    }
}
