using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

[TestFixture]
public sealed class TreeCatalogShadowsTests
{
    private static TreeCatalogEntry Entry(string treeId) => new()
    {
        TreeId = treeId,
        ShardCount = 4,
        Config = new TreeConfigSummary { ShardCount = 4, VirtualShardCount = 64, WalPartitions = 2 },
    };

    [Test]
    public void Of_names_resize_remediation_and_restore_copies_but_not_a_hand_aliased_tree()
    {
        var shadows = TreeCatalogShadows.Of(
        [
            Entry("orders") with { IsAlias = true, PhysicalTreeId = "orders/resized/7f3a" },
            Entry("orders/resized/7f3a"),
            Entry("audit") with { IsAlias = true, PhysicalTreeId = "audit/remediated/2" },
            Entry("audit/remediated/2"),
            Entry("ledger-bkprestore-1") with { RestoreShadowOfTreeId = "ledger" },
            Entry("sales") with { IsAlias = true, PhysicalTreeId = "factory-orders" },
            Entry("factory-orders"),
        ]);

        Assert.That(shadows, Is.EquivalentTo(new[] { "orders/resized/7f3a", "audit/remediated/2", "ledger-bkprestore-1" }));
    }

    [Test]
    public void Of_rejects_null() =>
        Assert.That(() => TreeCatalogShadows.Of(null!), Throws.ArgumentNullException);

    [Test]
    [TestCase("orders", "orders/resized/1", true)]
    [TestCase("a/crm/orders", "a/crm/orders/remediated/x", true)]
    [TestCase("orders", "orders-resized-1", false)]
    [TestCase("orders", "orders/", false)]
    [TestCase("orders", "ordersx/resized/1", false)]
    [TestCase("orders", "factory-orders", false)]
    public void IsDerivedCopy_requires_a_copy_named_beneath_the_logical_tree(string logical, string physical, bool expected) =>
        Assert.That(TreeCatalogShadows.IsDerivedCopy(logical, physical), Is.EqualTo(expected));
}
