using NSubstitute;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainSplitTests
{
    [Test]
    public async Task SealMovedSlotsAndEnterRejectAsync_waits_for_leaf_seals_then_persists_reject()
    {
        var h = CreateHarness();
        var slots = new[] { 0, 2 };
        await h.Grain.BeginSplitAsync(1, slots, VirtualShardCount);
        var writesBefore = h.State.WriteCount;
        var sealedLeaf = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Leaf.MarkSlotsMovedAwayAsync(slots, VirtualShardCount).Returns(sealedLeaf.Task);

        var freeze = h.Grain.SealMovedSlotsAndEnterRejectAsync(slots, VirtualShardCount);
        Assert.That(freeze.IsCompleted, Is.False);
        Assert.That(h.State.State.SplitInProgress!.Phase, Is.Not.EqualTo(ShardSplitPhase.Reject));
        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore));

        sealedLeaf.SetResult();
        await freeze;
        Assert.That(h.State.State.SplitInProgress!.Phase, Is.EqualTo(ShardSplitPhase.Reject));
        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore + 1));
        await h.Grain.SealMovedSlotsAndEnterRejectAsync(slots, VirtualShardCount);
        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore + 1), "Reject remains idempotent");
    }

    [Test]
    public async Task SealMovedSlotsAndEnterRejectAsync_failed_seal_does_not_enter_reject_and_retries()
    {
        var h = CreateHarness();
        var slots = new[] { 0, 2 };
        await h.Grain.BeginSplitAsync(1, slots, VirtualShardCount);
        h.Leaf.MarkSlotsMovedAwayAsync(slots, VirtualShardCount)
            .Returns(Task.FromException(new TimeoutException("seal failed")));

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.SealMovedSlotsAndEnterRejectAsync(slots, VirtualShardCount));
        Assert.That(h.State.State.SplitInProgress!.Phase, Is.Not.EqualTo(ShardSplitPhase.Reject));

        h.Leaf.MarkSlotsMovedAwayAsync(slots, VirtualShardCount).Returns(Task.CompletedTask);
        await h.Grain.SealMovedSlotsAndEnterRejectAsync(slots, VirtualShardCount);
        Assert.That(h.State.State.SplitInProgress!.Phase, Is.EqualTo(ShardSplitPhase.Reject));
    }
}
