using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

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
    public void AppendTxTerminalAsync_routed_through_the_alias_after_the_fence_is_rejected_and_not_forwarded()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, "logical-tree");
        try
        {
            Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true));
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        }

        h.ShadowTarget.DidNotReceiveWithAnyArgs().AppendTxTerminalAsync(default, default);
    }

    [Test]
    public async Task AppendTxTerminalAsync_addressed_to_the_fenced_copy_directly_is_applied_and_forwarded()
    {
        // A saga sends its terminals to the physical copy holding its prepares,
        // without a routed-logical stamp. The fenced copy takes the decision on
        // every shard and mirrors it, so the batch is not left decided on some of
        // its shards and pending on others for a resize undo to expose (#4369).
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public async Task AppendTxTerminalAsync_addressed_to_a_soft_deleted_fenced_copy_is_applied_and_forwarded()
    {
        // The resize soft-deletes the copy it retired, but an undo can still
        // recover it; a saga's terminal must not be left off some of its shards.
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        h.State.State.IsDeleted = true;
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.ShadowTarget.Received(1).AppendTxTerminalAsync(
            txid, true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public void SetManyAsync_without_a_binding_is_rejected_by_a_soft_deleted_fenced_copy()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        h.State.State.IsDeleted = true;
        List<KeyValuePair<string, byte[]>> entries = [new("k1", [1])];

        Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.SetManyAsync(entries));
    }

    [Test]
    public async Task SetManyAsync_prepared_by_a_saga_bound_to_the_fenced_copy_is_applied_and_forwarded()
    {
        var h = CreateHarness();
        h.ShadowTarget.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        List<KeyValuePair<string, byte[]>> entries = [new("k1", [1])];

        using (LatticePreparedContext.BeginScope())
        using (LatticeAtomicBindingContext.With(TreeId))
        {
            LatticeTransactionContext.Set(Guid.NewGuid());
            try
            {
                await h.Grain.SetManyAsync(entries);
            }
            finally
            {
                LatticeTransactionContext.Set(Guid.Empty);
            }
        }

        await h.ShadowTarget.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public void SetManyAsync_prepared_by_a_saga_bound_elsewhere_is_rejected_by_the_fenced_copy()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        List<KeyValuePair<string, byte[]>> entries = [new("k1", [1])];

        using (LatticePreparedContext.BeginScope())
        using (LatticeAtomicBindingContext.With(DestTreeId))
        {
            Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.SetManyAsync(entries));
        }

        h.ShadowTarget.DidNotReceiveWithAnyArgs().SetManyAsync(default!);
    }

    [Test]
    public void SetManyAsync_carrying_a_binding_outside_a_prepared_scope_is_rejected_by_the_fenced_copy()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        List<KeyValuePair<string, byte[]>> entries = [new("k1", [1])];

        using (LatticeAtomicBindingContext.With(TreeId))
        {
            Assert.ThrowsAsync<StaleTreeRoutingException>(() => h.Grain.SetManyAsync(entries));
        }
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task GetMirrorDestinationAsync_names_the_destination_while_the_copy_mirrors(int phaseIndex)
    {
        var phase = new[] { ShadowForwardPhase.Draining, ShadowForwardPhase.Drained, ShadowForwardPhase.Rejecting }[phaseIndex];
        var h = CreateHarness();
        SetShadowPhase(h.State, phase);

        Assert.That(await h.Grain.GetMirrorDestinationAsync(), Is.EqualTo(DestTreeId));
    }

    [Test]
    public async Task GetMirrorDestinationAsync_is_null_without_a_mirror_and_survives_the_soft_delete()
    {
        var h = CreateHarness();
        Assert.That(await h.Grain.GetMirrorDestinationAsync(), Is.Null);

        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        h.State.State.IsDeleted = true;
        Assert.That(await h.Grain.GetMirrorDestinationAsync(), Is.EqualTo(DestTreeId));
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
