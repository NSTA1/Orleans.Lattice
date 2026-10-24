using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class TreeMaintenanceProbeTests
{
    private const string TreeId = "probe-tree";

    private static (IGrainFactory Factory, ITreeResizeGrain Resize, ITreeReshardGrain Reshard, ITreeMergeGrain Merge, ITreeSnapshotGrain Snapshot) CreateFactory(
        bool resizeIdle = true, bool reshardIdle = true, bool mergeComplete = true, bool snapshotIdle = true)
    {
        var factory = Substitute.For<IGrainFactory>();
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(resizeIdle);
        factory.GetGrain<ITreeResizeGrain>(TreeId).Returns(resize);
        var reshard = Substitute.For<ITreeReshardGrain>();
        reshard.IsIdleAsync().Returns(reshardIdle);
        factory.GetGrain<ITreeReshardGrain>(TreeId).Returns(reshard);
        var merge = Substitute.For<ITreeMergeGrain>();
        merge.IsCompleteAsync().Returns(mergeComplete);
        factory.GetGrain<ITreeMergeGrain>(TreeId).Returns(merge);
        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        snapshot.IsIdleAsync().Returns(snapshotIdle);
        factory.GetGrain<ITreeSnapshotGrain>(TreeId).Returns(snapshot);
        return (factory, resize, reshard, merge, snapshot);
    }

    [Test]
    public async Task IsMaintenanceInFlightAsync_is_false_when_every_coordinator_is_idle()
    {
        var (factory, resize, reshard, merge, snapshot) = CreateFactory();

        var inFlight = await TreeMaintenanceProbe.IsMaintenanceInFlightAsync(factory, TreeId);

        Assert.That(inFlight, Is.False);
        await resize.Received(1).IsIdleAsync();
        await reshard.Received(1).IsIdleAsync();
        await merge.Received(1).IsCompleteAsync();
        await snapshot.Received(1).IsIdleAsync();
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [TestCase(false, true, true, true)]
    [TestCase(true, false, true, true)]
    [TestCase(true, true, false, true)]
    [TestCase(true, true, true, false)]
    public async Task IsMaintenanceInFlightAsync_is_true_when_any_coordinator_is_busy(
        bool resizeIdle, bool reshardIdle, bool mergeComplete, bool snapshotIdle)
    {
        var (factory, _, _, _, _) = CreateFactory(resizeIdle, reshardIdle, mergeComplete, snapshotIdle);

        var inFlight = await TreeMaintenanceProbe.IsMaintenanceInFlightAsync(factory, TreeId);

        Assert.That(inFlight, Is.True);
    }

    [Test]
    public async Task IsMaintenanceInFlightAsync_short_circuits_after_the_first_busy_coordinator()
    {
        var (factory, _, reshard, merge, snapshot) = CreateFactory(resizeIdle: false);

        await TreeMaintenanceProbe.IsMaintenanceInFlightAsync(factory, TreeId);

        await reshard.DidNotReceive().IsIdleAsync();
        await merge.DidNotReceive().IsCompleteAsync();
        await snapshot.DidNotReceive().IsIdleAsync();
    }

    [Test]
    public void IsMaintenanceInFlightAsync_rejects_a_reserved_system_tree_like_the_lattice_verbs()
    {
        var (factory, resize, _, _, _) = CreateFactory();
        var systemTree = LatticeConstants.SystemTreePrefix + "registry-probe";

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            () => TreeMaintenanceProbe.IsMaintenanceInFlightAsync(factory, systemTree));
        resize.DidNotReceive().IsIdleAsync();
    }

    [TestCase(true, true, false)]
    [TestCase(false, true, true)]
    [TestCase(true, false, true)]
    public async Task IsSnapshotOrMergeInFlightAsync_probes_only_snapshot_then_merge(
        bool snapshotIdle, bool mergeComplete, bool expected)
    {
        var (factory, resize, reshard, merge, snapshot) = CreateFactory(
            snapshotIdle: snapshotIdle, mergeComplete: mergeComplete);

        Assert.That(await TreeMaintenanceProbe.IsSnapshotOrMergeInFlightAsync(factory, TreeId), Is.EqualTo(expected));

        await snapshot.Received(1).IsIdleAsync();
        await merge.Received(snapshotIdle ? 1 : 0).IsCompleteAsync();
        await resize.DidNotReceive().IsIdleAsync();
        await reshard.DidNotReceive().IsIdleAsync();
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public void IsSnapshotOrMergeInFlightAsync_rejects_system_trees_before_any_grain_call()
    {
        var (factory, _, _, merge, snapshot) = CreateFactory();

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(() =>
            TreeMaintenanceProbe.IsSnapshotOrMergeInFlightAsync(factory, LatticeConstants.SystemTreePrefix + "probe"));

        snapshot.DidNotReceive().IsIdleAsync();
        merge.DidNotReceive().IsCompleteAsync();
    }

    [Test]
    public void IsSnapshotOrMergeInFlightAsync_propagates_snapshot_failure_without_probing_merge()
    {
        var (factory, _, _, merge, snapshot) = CreateFactory();
        snapshot.IsIdleAsync().Returns(Task.FromException<bool>(new InvalidOperationException("unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            TreeMaintenanceProbe.IsSnapshotOrMergeInFlightAsync(factory, TreeId));

        merge.DidNotReceive().IsCompleteAsync();
    }
}
