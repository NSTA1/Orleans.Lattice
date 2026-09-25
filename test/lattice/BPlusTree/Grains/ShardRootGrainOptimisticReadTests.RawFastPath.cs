using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [TestCase("none")]
    [TestCase("point")]
    [TestCase("routing")]
    public async Task Raw_leaf_fault_propagates_only_without_an_overlapping_write(string overlap)
    {
        var (grain, _, leaf, _) = CreateGrain();
        var raw = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetAsync("k").Returns(raw.Task);
        var read = grain.TryGetOptimisticAsync("k");
        if (overlap == "point")
            await ((IIncomingGrainCallFilter)grain).Invoke(
                CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetAsync), Task.CompletedTask));
        if (overlap == "routing")
        {
            grain.BeginRoutingMutation();
            grain.EndRoutingMutation();
        }
        raw.SetException(new InvalidOperationException("leaf failed"));
        if (overlap == "none")
            Assert.ThrowsAsync<InvalidOperationException>(async () => await read);
        else
            Assert.That((await read).IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Raw_present_reply_after_a_point_write_started_requires_a_new_proved_observation(bool finishWrite)
    {
        var (grain, _, leaf, _) = CreateGrain();
        var raw = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pendingWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetAsync("k").Returns(raw.Task);
        leaf.GetWithVersionAsync("k").Returns(Stamped(Bytes("after")));
        var read = grain.TryGetOptimisticAsync("k");
        var call = CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetAsync),
            finishWrite ? Task.CompletedTask : pendingWrite.Task);
        var write = ((IIncomingGrainCallFilter)grain).Invoke(call);
        raw.SetResult(Bytes("before"));

        Assert.That((await read).Value, Is.EqualTo(Bytes("after")));
        await leaf.Received(1).GetWithVersionAsync("k");
        pendingWrite.TrySetResult();
        await write;
    }

    [Test]
    public async Task Raw_reply_after_a_completed_point_write_cannot_validate_without_leaf_proof()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var raw = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetAsync("k").Returns(raw.Task);
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue { Value = Bytes("unproved") });
        var read = grain.TryGetOptimisticAsync("k");
        await ((IIncomingGrainCallFilter)grain).Invoke(
            CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetAsync), Task.CompletedTask));
        raw.SetResult(Bytes("before"));
        Assert.That((await read).IsValidated, Is.False);
    }

    [Test]
    public async Task Raw_present_reply_after_routing_change_retries_without_requesting_proof()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var raw = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetAsync("k").Returns(raw.Task);
        var read = grain.TryGetOptimisticAsync("k");
        grain.BeginRoutingMutation();
        grain.EndRoutingMutation();
        raw.SetResult(Bytes("before"));
        Assert.That((await read).IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Raw_miss_then_written_returns_the_later_proved_value()
    {
        var (grain, _, leaf, _) = CreateGrain();
        leaf.GetAsync("k").Returns((byte[]?)null);
        leaf.GetWithVersionAsync("k").Returns(Stamped(Bytes("written")));
        Assert.That((await grain.TryGetOptimisticAsync("k")).Value, Is.EqualTo(Bytes("written")));
        await leaf.Received(1).GetAsync("k");
        await leaf.Received(1).GetWithVersionAsync("k");
    }

    [Test]
    public async Task Already_admitted_write_skips_raw_rpc()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = ((IIncomingGrainCallFilter)grain).Invoke(
            CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetAsync), gate.Task));
        try
        {
            leaf.GetWithVersionAsync("k").Returns(Stamped(Bytes("present")));
            Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
            await leaf.DidNotReceive().GetAsync(Arg.Any<string>());
        }
        finally
        {
            gate.TrySetResult();
            await write;
        }
    }
}
