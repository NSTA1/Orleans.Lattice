using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeResizeGrainTests
{
    [Test]
    public async Task RunPendingUndoAsync_preserves_domain_refusal_and_pending_intent()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        var refusal = new LatticeShuttingDownException("snapshot state writer is stopping");
        h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).AbortAsync(UndoSnapshotSuffix)
            .ThrowsAsync(refusal);
        await h.Grain.RequestUndoAsync();

        var actual = Assert.ThrowsAsync<LatticeShuttingDownException>(() => h.Grain.RunPendingUndoAsync());
        var progress = await h.Grain.GetUndoProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(actual, Is.SameAs(refusal));
            Assert.That(progress.Pending, Is.True);
            Assert.That(progress.FailedOperationId, Is.Null);
            Assert.That(h.Intent.State.RequestedOperationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(h.State.State.InProgress, Is.True);
        });
    }

    [Test]
    public async Task RunPendingUndoAsync_preserves_domain_refusal_from_bootstrap_fence_read()
    {
        var h = CreateUndoHarness(ResizePhase.Snapshot);
        await h.Grain.RequestUndoAsync();
        var refusal = new LatticeShuttingDownException("registry is stopping");
        h.GrainFactory.GetLatticeRegistry().GetEntryAsync(TreeId).ThrowsAsync(refusal);

        var actual = Assert.ThrowsAsync<LatticeShuttingDownException>(() => h.Grain.RunPendingUndoAsync());

        Assert.Multiple(() =>
        {
            Assert.That(actual, Is.SameAs(refusal));
            Assert.That(h.Intent.State.RequestedOperationId, Is.EqualTo(UndoSnapshotSuffix));
            Assert.That(h.Intent.State.FailedOperationId, Is.Null);
            Assert.That(h.State.State.InProgress, Is.True);
        });
        await h.GrainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).DidNotReceive().AbortAsync(Arg.Any<string>());
    }
}
