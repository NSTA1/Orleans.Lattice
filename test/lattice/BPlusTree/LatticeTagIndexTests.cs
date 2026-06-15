using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for the tag-index entry points
/// (<see cref="LatticeTagIndexExtensions"/>) and value types
/// (<see cref="TagReconcileReport"/>, <see cref="TaggedKey"/>,
/// <see cref="TagConsistency"/>): argument validation and value semantics that
/// do not require a live cluster.
/// </summary>
[TestFixture]
public class LatticeTagIndexTests
{
    [Test]
    public void TagIndex_throws_on_null_tree()
    {
        ILattice tree = null!;
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => tree.TagIndex(factory, "idx"), Throws.ArgumentNullException);
    }

    [Test]
    public void TagIndex_throws_on_null_factory()
    {
        var tree = Substitute.For<ILattice>();
        Assert.That(() => tree.TagIndex(null!, "idx"), Throws.ArgumentNullException);
    }

    [Test]
    public void TagIndex_throws_on_empty_index_name()
    {
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => tree.TagIndex(factory, ""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => tree.TagIndex(factory, null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MultiTreeTagIndex_throws_on_null_factory()
    {
        IGrainFactory factory = null!;
        Assert.That(() => factory.MultiTreeTagIndex("idx"), Throws.ArgumentNullException);
    }

    [Test]
    public void MultiTreeTagIndex_throws_on_empty_index_name()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => factory.MultiTreeTagIndex(""), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => factory.MultiTreeTagIndex(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void TagReconcileReport_Empty_is_all_zero()
    {
        var report = TagReconcileReport.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(report.TreesCovered, Is.Zero);
            Assert.That(report.KeysScanned, Is.Zero);
            Assert.That(report.MembershipRowsScanned, Is.Zero);
            Assert.That(report.OrphanRowsRemoved, Is.Zero);
        });
    }

    [Test]
    public void TagReconcileReport_Combine_sums_each_counter()
    {
        var a = new TagReconcileReport(1, 2, 3, 4);
        var b = new TagReconcileReport(10, 20, 30, 40);
        var sum = a.Combine(b);
        Assert.That(sum, Is.EqualTo(new TagReconcileReport(11, 22, 33, 44)));
    }

    [Test]
    public void TaggedKey_has_value_equality()
    {
        Assert.That(new TaggedKey("t", "k"), Is.EqualTo(new TaggedKey("t", "k")));
        Assert.That(new TaggedKey("t", "k"), Is.Not.EqualTo(new TaggedKey("t", "k2")));
    }

    [Test]
    public void TagConsistency_default_is_eventual()
    {
        Assert.That(default(TagConsistency), Is.EqualTo(TagConsistency.Eventual));
    }

    // ── Flag membership descriptor validation ────────────────────────

    [Test]
    public void TagIndex_or_flag_membership_without_replica_id_throws()
    {
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => tree.TagIndex(factory, "idx", membershipMode: LatticeMergeMode.OrFlag),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void TagIndex_rw_flag_membership_with_empty_replica_id_throws()
    {
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => tree.TagIndex(factory, "idx", membershipMode: LatticeMergeMode.RwFlag, replicaId: ""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void TagIndex_unsupported_membership_mode_throws()
    {
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => tree.TagIndex(factory, "idx", membershipMode: LatticeMergeMode.OrSet, replicaId: "site-a"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MultiTreeTagIndex_or_flag_membership_without_replica_id_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => factory.MultiTreeTagIndex("idx", membershipMode: LatticeMergeMode.OrFlag),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MultiTreeTagIndex_unsupported_membership_mode_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(
            () => factory.MultiTreeTagIndex("idx", membershipMode: LatticeMergeMode.PnCounter, replicaId: "site-a"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MultiTreeTagIndex_flag_membership_with_replica_id_is_accepted()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Substitute.For<ILattice>());
        var idx = factory.MultiTreeTagIndex("idx", membershipMode: LatticeMergeMode.OrFlag, replicaId: "site-a");
        Assert.That(idx, Is.Not.Null);
    }

    [Test]
    public void MultiTreeTagIndex_lww_membership_is_the_default()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Substitute.For<ILattice>());
        var idx = factory.MultiTreeTagIndex("idx");
        Assert.That(idx, Is.Not.Null);
    }
}
