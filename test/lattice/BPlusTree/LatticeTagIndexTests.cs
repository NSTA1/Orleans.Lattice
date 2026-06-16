using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for the tag-index entry point
/// (<see cref="ILatticeTagIndexFactory"/>) and value types
/// (<see cref="TagReconcileReport"/>, <see cref="TaggedKey"/>,
/// <see cref="TagConsistency"/>): argument validation and value semantics that
/// do not require a live cluster.
/// </summary>
[TestFixture]
public class LatticeTagIndexTests
{
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

    // ── Flag membership descriptor validation (via the injected factory) ──

    [Test]
    public void TagIndex_or_flag_membership_without_replica_id_throws()
    {
        var tree = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var ctx = FakeLatticeReplicationContext.Enabled(replicaId: string.Empty, mode: LatticeMergeMode.OrFlag);
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, ctx);
        Assert.That(
            () => factory.Create(tree, "idx"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void TagIndex_rw_flag_membership_with_empty_replica_id_throws()
    {
        var tree = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var ctx = FakeLatticeReplicationContext.Enabled(replicaId: string.Empty, mode: LatticeMergeMode.RwFlag);
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, ctx);
        Assert.That(
            () => factory.Create(tree, "idx"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MultiTreeTagIndex_or_flag_membership_without_replica_id_throws()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var ctx = FakeLatticeReplicationContext.Enabled(replicaId: string.Empty, mode: LatticeMergeMode.OrFlag);
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, ctx);
        Assert.That(
            () => factory.CreateMultiTree("idx"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void MultiTreeTagIndex_non_flag_declared_mode_falls_back_to_lww()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Substitute.For<ILattice>());
        var ctx = FakeLatticeReplicationContext.Enabled(replicaId: "site-a", mode: LatticeMergeMode.PnCounter);
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, ctx);
        Assert.That(() => factory.CreateMultiTree("idx"), Throws.Nothing);
    }

    [Test]
    public void MultiTreeTagIndex_flag_membership_with_replica_id_is_accepted()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Substitute.For<ILattice>());
        var ctx = FakeLatticeReplicationContext.Enabled(replicaId: "site-a", mode: LatticeMergeMode.OrFlag);
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, ctx);
        var idx = factory.CreateMultiTree("idx");
        Assert.That(idx, Is.Not.Null);
    }

    [Test]
    public void MultiTreeTagIndex_disabled_replication_context_uses_lww()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(Substitute.For<ILattice>());
        var factory = new DefaultLatticeTagIndexFactory(grainFactory, FakeLatticeReplicationContext.Disabled);
        var idx = factory.CreateMultiTree("idx");
        Assert.That(idx, Is.Not.Null);
    }

    [Test]
    public void Factory_create_rejects_null_tree()
    {
        var factory = new DefaultLatticeTagIndexFactory(
            Substitute.For<IGrainFactory>(), FakeLatticeReplicationContext.Disabled);
        Assert.That(() => factory.Create(null!, "idx"), Throws.ArgumentNullException);
    }

    [Test]
    public void Factory_create_rejects_empty_index_name()
    {
        var factory = new DefaultLatticeTagIndexFactory(
            Substitute.For<IGrainFactory>(), FakeLatticeReplicationContext.Disabled);
        Assert.That(() => factory.Create(Substitute.For<ILattice>(), ""), Throws.ArgumentException);
    }

    [Test]
    public void Factory_create_multi_tree_rejects_empty_index_name()
    {
        var factory = new DefaultLatticeTagIndexFactory(
            Substitute.For<IGrainFactory>(), FakeLatticeReplicationContext.Disabled);
        Assert.That(() => factory.CreateMultiTree(""), Throws.ArgumentException);
    }
}
