using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A mutation that passed the reject gate before the shard entered
/// <see cref="ShadowForwardPhase.Rejecting"/> is still mirrored to the
/// destination. A resize fences the old copy before it moves the alias, so the
/// old copy is still the live one when the phase flips: dropping the mirror of
/// an operation it had already accepted would leave an acknowledged write on a
/// copy no router reads once the alias moves. A shard the coordinator un-fences
/// after a failed swap (<see cref="IShardRootGrain.ExitRejectingAsync"/>) serves
/// and mirrors again.
/// </summary>
public partial class ShardRootGrainShadowForwardTests
{
    [Test]
    public async Task AppendTxTerminalAsync_still_forwards_when_the_shard_enters_rejecting_mid_call()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);
        var clock = new TaskCompletionSource<HybridLogicalClock>(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Leaf.GetClockAsync().Returns(clock.Task);
        var txid = Guid.NewGuid();

        // The terminal passes the gate, then yields on the leaf clock fan-out;
        // the coordinator's fence lands in that gap.
        var terminal = h.Grain.AppendTxTerminalAsync(txid, committed: true);
        await h.Grain.EnterRejectingAsync(OperationId);
        clock.SetResult(HybridLogicalClock.Zero);
        await terminal;

        await h.ShadowTarget.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public void AppendTxTerminalAsync_arriving_after_the_fence_is_rejected_and_not_forwarded()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true));

        h.ShadowTarget.DidNotReceiveWithAnyArgs().AppendTxTerminalAsync(default, default);
    }

    [Test]
    public async Task ExitRejectingAsync_returns_a_rejecting_shard_to_drained_and_it_serves_and_forwards_again()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        await h.Grain.ExitRejectingAsync(OperationId);
        await h.Grain.SetAsync("k", [1]);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Drained));
        Assert.That(h.State.WriteCount, Is.GreaterThan(0));
        await h.ShadowTarget.Received(1).SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task ExitRejectingAsync_is_a_no_op_when_the_shard_is_not_rejecting()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.ExitRejectingAsync(OperationId);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Draining));
    }

    [Test]
    public async Task ExitRejectingAsync_is_a_no_op_without_shadow_forward_state()
    {
        var h = CreateHarness();

        await h.Grain.ExitRejectingAsync(OperationId);

        Assert.That(h.State.State.ShadowForward, Is.Null);
    }

    [Test]
    public void ExitRejectingAsync_refuses_a_different_operationId()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.ExitRejectingAsync("op-other"),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Rejecting));
    }
}
