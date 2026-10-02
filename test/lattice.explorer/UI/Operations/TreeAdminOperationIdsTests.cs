using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.Tests.UI.Operations;

/// <summary>
/// The ids the Explorer gives the tree-administration operations it starts (#4124):
/// valid operation ids that name their kind and target, so a page opened later finds
/// the one still running for what it shows and never one for anything else.
/// </summary>
[TestFixture]
public sealed class TreeAdminOperationIdsTests
{
    [Test]
    public void A_new_id_is_a_valid_operation_id_that_names_its_kind_and_is_fresh_every_time()
    {
        var first = TreeAdminOperationIds.New(TreeAdminOperationKinds.OrphanedLeavesRepair, "a view name / with \u00e9 any characters");
        var second = TreeAdminOperationIds.New(TreeAdminOperationKinds.OrphanedLeavesRepair, "a view name / with \u00e9 any characters");

        Assert.Multiple(() =>
        {
            Assert.That(first, Does.Match("^orphaned-leaves-repair-[0-9a-f]{16}-[0-9a-f]{32}$"));
            Assert.That(first.Length, Is.LessThanOrEqualTo(128));
            Assert.That(second, Is.Not.EqualTo(first));
            Assert.That(first, Does.StartWith(TreeAdminOperationIds.PrefixOf(TreeAdminOperationKinds.OrphanedLeavesRepair, "a view name / with \u00e9 any characters")));
            Assert.That(TreeAdminOperationIds.PrefixOf(TreeAdminOperationKinds.WalMove, "t"), Does.Match("^wal-move-[0-9a-f]{16}-$"));
        });
    }

    [Test]
    public void An_id_matches_only_its_own_kind_and_target()
    {
        var id = TreeAdminOperationIds.New(TreeAdminOperationKinds.ViewRebuild, "by-status");

        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.ViewRebuild, "by-status"), Is.True);
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.ViewRebuild, "by-statu"), Is.False);
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.ViewReconcile, "by-status"), Is.False);
            Assert.That(TreeAdminOperationIds.Matches("0123456789abcdef0123456789abcdef", TreeAdminOperationKinds.ViewRebuild, "by-status"), Is.False);
        });
    }

    [Test]
    public void A_wal_target_is_its_tree_and_partition_together()
    {
        var id = TreeAdminOperationIds.New(TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target("orders", 1));

        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminOperationIds.Target("orders", 12), Is.EqualTo("orders\n12"));
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target("orders", 1)), Is.True);
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target("orders", 11)), Is.False);
            Assert.That(TreeAdminOperationIds.Matches(id, TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target("orders1", 1)), Is.False);
        });
    }

    [Test]
    public void The_arguments_are_checked()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TreeAdminOperationIds.New(string.Empty, "t"), Throws.ArgumentException);
            Assert.That(() => TreeAdminOperationIds.New(TreeAdminOperationKinds.WalMove, null!), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationIds.Matches(null!, TreeAdminOperationKinds.WalMove, "t"), Throws.ArgumentNullException);
            Assert.That(() => TreeAdminOperationIds.Target(null!, 0), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Only_a_facade_that_runs_tracked_operations_offers_them()
    {
        var both = NSubstitute.Substitute.For<ILatticeTreeAdmin, ILatticeTreeAdminOperations>();
        var blockingOnly = NSubstitute.Substitute.For<ILatticeTreeAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminOperationsAccess.Of(both), Is.SameAs(both));
            Assert.That(TreeAdminOperationsAccess.Of(blockingOnly), Is.Null);
            Assert.That(TreeAdminOperationsAccess.Of(null), Is.Null);
        });
    }
}
