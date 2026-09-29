using Orleans.Lattice.Explorer.Shell.Areas.Cluster;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Cluster;

/// <summary>
/// The Cluster address grammar: every page has one canonical address, every
/// address reads back to its page, and a tree whose last part is a view word is
/// still reachable without ambiguity.
/// </summary>
[TestFixture]
public sealed class ClusterAddressesTests
{
    [Test]
    public void The_area_roots_format_canonically()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterAddresses.Overview.Format(), Is.EqualTo("/cluster"));
            Assert.That(ClusterAddresses.Trees.Format(), Is.EqualTo("/cluster/trees"));
            Assert.That(ClusterAddresses.Wal().Format(), Is.EqualTo("/cluster/wal"));
            Assert.That(ClusterAddresses.Orphans().Format(), Is.EqualTo("/cluster/orphans"));
        });
    }

    [Test]
    [TestCase("a/crm/orders", "Overview", "/cluster/trees/a/crm/orders")]
    [TestCase("a/crm/orders", "Tools", "/cluster/trees/a/crm/orders/tools")]
    [TestCase("orders", "Reshard", "/cluster/trees/orders/reshard")]
    [TestCase("t/acme/orders", "Resize", "/cluster/trees/t/acme/orders/resize")]
    [TestCase("orders", "Snapshot", "/cluster/trees/orders/snapshot")]
    [TestCase("tools", "Overview", "/cluster/trees/tools")]
    [TestCase("jobs/tools", "Overview", "/cluster/trees/jobs/tools/overview")]
    [TestCase("jobs/overview", "Overview", "/cluster/trees/jobs/overview/overview")]
    public void A_tree_view_round_trips_through_its_address(string treeId, string viewName, string expected)
    {
        var view = Enum.Parse<ClusterTreeView>(viewName);
        var address = ClusterAddresses.Tree(treeId, view);
        var parsed = ClusterAddresses.Parse(ExplorerAddress.Parse(address.Format()));

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo(expected));
            Assert.That(parsed, Is.EqualTo(new ClusterLocation(ClusterPageKind.Tree, treeId, view)));
        });
    }

    [Test]
    public void Upper_case_tree_parts_are_encoded_and_decoded()
    {
        var address = ClusterAddresses.Tree("Orders");

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo("/cluster/trees/%4Frders"));
            Assert.That(ClusterAddresses.Parse(ExplorerAddress.Parse(address.Format()))!.TreeId, Is.EqualTo("Orders"));
        });
    }

    [Test]
    [TestCase("")]
    [TestCase("a//b")]
    [TestCase("/orders")]
    [TestCase("a/b/c/d/e/f/g/h")]
    public void A_tree_with_no_address_says_so(string treeId)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterAddresses.TryTree(treeId, ClusterTreeView.Overview, out var address), Is.False);
            Assert.That(address, Is.Null);
            Assert.That(() => ClusterAddresses.Tree(treeId), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_deepest_routable_tree_has_an_address_and_its_views_do_not_overflow()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClusterAddresses.TryTree("a/b/c/d/e/f/g", ClusterTreeView.Overview, out _), Is.True);
            Assert.That(ClusterAddresses.TryTree("a/b/c/d/e/f/g", ClusterTreeView.Tools, out _), Is.False);
            Assert.That(ClusterAddresses.TryTree("a/b/c/d/e/f", ClusterTreeView.Tools, out _), Is.True);
        });
    }

    [Test]
    public void The_wal_page_carries_its_plan_in_the_query()
    {
        var address = ClusterAddresses.Wal("a/crm/orders", 3, "blob-b");
        var parsed = ClusterAddresses.Parse(ExplorerAddress.Parse(address.Format()));

        Assert.That(parsed, Is.EqualTo(new ClusterLocation(ClusterPageKind.Wal, "a/crm/orders", Partition: 3, Target: "blob-b")));
    }

    [Test]
    public void The_orphans_page_carries_its_tree_in_the_query()
    {
        var parsed = ClusterAddresses.Parse(ExplorerAddress.Parse(ClusterAddresses.Orphans("orders").Format()));

        Assert.That(parsed, Is.EqualTo(new ClusterLocation(ClusterPageKind.Orphans, "orders")));
    }

    [Test]
    [TestCase("/cluster/unknown")]
    [TestCase("/cluster/wal/extra")]
    [TestCase("/cluster/orphans/extra")]
    [TestCase("/data/trees")]
    public void An_address_that_names_no_page_reads_as_none(string text)
    {
        Assert.That(ClusterAddresses.Parse(ExplorerAddress.Parse(text)), Is.Null);
    }

    [Test]
    public void A_bad_partition_query_is_ignored()
    {
        var parsed = ClusterAddresses.Parse(ExplorerAddress.Parse("/cluster/wal?tree=orders&partition=-1"));

        Assert.That(parsed, Is.EqualTo(new ClusterLocation(ClusterPageKind.Wal, "orders")));
    }
}
