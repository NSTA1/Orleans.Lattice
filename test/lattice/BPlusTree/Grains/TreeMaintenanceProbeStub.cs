using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Wires the four maintenance coordinators that
/// <c>TreeMaintenanceProbe</c> reads so each answers with whatever the
/// corresponding <see cref="ILattice"/> status verb is stubbed to return at call
/// time. A test keeps arranging maintenance state on the <see cref="ILattice"/>
/// substitute (for example <c>lattice.IsResizeCompleteAsync().Returns(false)</c>)
/// while the grain under test probes the coordinators directly.
/// </summary>
internal static class TreeMaintenanceProbeStub
{
    /// <summary>Bridges the coordinators for <paramref name="treeId"/> onto <paramref name="lattice"/>'s status stubs.</summary>
    public static void BridgeToLattice(IGrainFactory grainFactory, string treeId, ILattice lattice)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(_ => lattice.IsResizeCompleteAsync());
        grainFactory.GetGrain<ITreeResizeGrain>(treeId).Returns(resize);

        var reshard = Substitute.For<ITreeReshardGrain>();
        reshard.IsIdleAsync().Returns(_ => lattice.IsReshardCompleteAsync());
        grainFactory.GetGrain<ITreeReshardGrain>(treeId).Returns(reshard);

        var merge = Substitute.For<ITreeMergeGrain>();
        merge.IsCompleteAsync().Returns(_ => lattice.IsMergeCompleteAsync());
        grainFactory.GetGrain<ITreeMergeGrain>(treeId).Returns(merge);

        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        snapshot.IsIdleAsync().Returns(_ => lattice.IsSnapshotCompleteAsync());
        grainFactory.GetGrain<ITreeSnapshotGrain>(treeId).Returns(snapshot);
    }
}
