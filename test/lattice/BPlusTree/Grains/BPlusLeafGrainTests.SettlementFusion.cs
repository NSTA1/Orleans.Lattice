using NSubstitute;
using NSubstitute.Extensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Local_forwarded_prepare_refuses_an_applied_terminal_even_when_the_fused_probe_faults()
    {
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        var reply = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Registry.Configure().IsDecidedOrForgottenForPrepareAsync(txid).Returns(_ =>
        {
            entered.SetResult();
            return reply.Task;
        });
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        var prepare = ForwardedPrepareSetAsync(leaf, txid, "k", [2]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await leaf.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        reply.SetException(new TimeoutException("participant lookup failed after status resolved"));
        await prepare;

        Assert.That(leaf.PendingTransactionCount, Is.Zero);
        Assert.That(leaf.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 1 }));
    }

    [TestCase((int)TxStatus.Committed, 0)]
    [TestCase((int)TxStatus.Aborted, 0)]
    [TestCase((int)TxStatus.InFlight, 1)]
    public async Task Self_terminalise_uses_one_settlement_dispatch_and_no_leaf_side_fallback(int status, int pending)
    {
        var txid = Guid.NewGuid();
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetStatusForSettlementAsync(txid).Returns((TxStatus)status);
        var (grain, state) = BuildSelfTerminaliseLeafCore(txid, registry, out _, persistedCheckpoint: 0);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(pending));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(pending == 0 ? 4 : 1));
        await registry.Received(1).GetStatusForSettlementAsync(txid);
        await registry.DidNotReceive().GetStatusForTerminalAsync(txid);
        await registry.DidNotReceive().GetRecordedStatusAsync(txid);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Local_forwarded_prepare_uses_one_conditional_dispatch(bool refused)
    {
        var h = BuildReactivatingLeaf(TxStatus.InFlight);
        var txid = Guid.NewGuid();
        h.Registry.Configure().IsDecidedOrForgottenForPrepareAsync(txid).Returns(refused);
        h.Registry.ClearReceivedCalls();
        var leaf = h.Activate();
        await leaf.SetAsync("k", [1]);

        await ForwardedPrepareSetAsync(leaf, txid, "k", [2]);

        Assert.That(leaf.PendingTransactionCount, Is.EqualTo(refused ? 0 : 1));
        await h.Registry.Received(1).IsDecidedOrForgottenForPrepareAsync(txid);
        await h.Registry.DidNotReceive().GetStatusForPrepareDedupAsync(txid);
        await h.Registry.DidNotReceive().GetParticipantsAsync(txid);
        await h.PhysicalRegistry.DidNotReceive().IsDecidedOrForgottenForPrepareAsync(txid);
    }
}
