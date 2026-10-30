using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Combined_parent_seed_waits_for_durable_binding_before_snapshot()
    {
        var (grain, state, parent) = CreateGrainWithParent();
        await grain.SetAsync("a", [1]);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () =>
        {
            entered.TrySetResult();
            return release.Task;
        };

        var seed = grain.SetParentAndGetChildDigestSnapshotAsync(LeafTestParentId);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(seed.IsCompleted, Is.False);
        }
        finally
        {
            release.TrySetResult();
        }
        var snapshot = await seed;

        Assert.That(state.State.ParentId, Is.EqualTo(LeafTestParentId));
        Assert.That(snapshot.EntryCount, Is.EqualTo(1));
        Assert.That(snapshot.Hash, Is.EqualTo(state.State.ProjectionHash));
        snapshot.Hash![0] ^= 0xFF;
        Assert.That(snapshot.Hash, Is.Not.EqualTo(state.State.ProjectionHash));
        await parent.DidNotReceiveWithAnyArgs().OnChildDigestPublishedAsync(default, default);
    }

    [Test]
    public async Task Combined_parent_seed_failed_write_rolls_back_and_retry_is_idempotent()
    {
        var (grain, state, parent) = CreateGrainWithParent();
        state.ThrowOnWrite = new IOException("seed write failed");

        Assert.ThrowsAsync<IOException>(() => grain.SetParentAndGetChildDigestSnapshotAsync(LeafTestParentId));
        Assert.That(state.State.ParentId, Is.Null);
        Assert.That(state.State.DigestPublishSequence, Is.Zero);

        var first = await grain.SetParentAndGetChildDigestSnapshotAsync(LeafTestParentId);
        var writes = state.WriteCount;
        var second = await grain.SetParentAndGetChildDigestSnapshotAsync(LeafTestParentId);

        Assert.That(state.WriteCount, Is.EqualTo(writes));
        Assert.That(second.PublishSequence, Is.GreaterThan(first.PublishSequence));
        await grain.SetParentAndGetChildDigestSnapshotAsync(null);
        Assert.That(state.State.ParentId, Is.Null);
        await parent.DidNotReceiveWithAnyArgs().OnChildDigestPublishedAsync(default, default);
    }
}
