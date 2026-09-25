using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static async Task<(ShardRootGrain Grain, IBPlusLeafGrain Leaf,
        IGrainContext Context, Func<Task> SuspendFlush)> CreateDeactivationHarnessAsync()
    {
        var context = Substitute.For<IGrainContext>();
        var registry = Substitute.For<ITimerRegistry>();
        registry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection().AddSingleton(registry).BuildServiceProvider();
        context.ActivationServices.Returns(services);
        var (grain, state, leaf, _) = CreateGrain(context: context);
        leaf.DeleteTrackedAsync(Arg.Any<string>()).Returns(new LeafDeleteResult { Deleted = true });
        await grain.DeleteAsync("dirty");
        var registration = registry.ReceivedCalls()
            .Single(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        var tick = (Func<CancellationToken, Task>)registration.GetArguments()[2]!;

        return (grain, leaf, context, async () =>
        {
            for (var i = 0; i < 5; i++)
            {
                state.ThrowOnWrite = new InconsistentStateException("stale shard version");
                await tick(CancellationToken.None);
            }
        });
    }

    private static Task InvokeGatedWrite(
        ShardRootGrain grain, IBPlusLeafGrain leaf, bool batch, TaskCompletionSource<SplitResult?> gate)
    {
        if (batch)
        {
            leaf.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(gate.Task);
        }

        var methodName = batch ? nameof(IShardRootGrain.SetManyAsync) : nameof(IShardRootGrain.SetAsync);
        var method = typeof(IShardRootGrain).GetMethods().First(m => m.Name == methodName);
        return ((IIncomingGrainCallFilter)grain).Invoke(QuiesceCallContext(methodName,
            batch ? () => grain.SetManyAsync([new("write", [1])]) : () => gate.Task, method));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Flush_suspension_fences_immediately_but_deactivates_only_after_in_flight_write_drains(bool batch)
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = InvokeGatedWrite(grain, leaf, batch, gate);
        Assert.That(write.IsCompleted, Is.False);

        await suspend();
        Assert.That(grain.DeactivationRequested, Is.True);
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);

        // The leaf's callback must remain runnable while the fenced write finishes.
        var callback = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.PublishLeafByteFootprintAsync))!;
        await ((IIncomingGrainCallFilter)grain).Invoke(QuiesceCallContext(
            callback.Name, () => Task.CompletedTask, callback));
        gate.SetResult(null);
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Write_admitted_after_flush_suspension_is_refused_before_leaf_dispatch(bool batch)
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = InvokeGatedWrite(grain, leaf, false, gate);
        await suspend();

        var refused = Assert.ThrowsAsync<ShardRootDeactivatingException>(() =>
            InvokeGatedWrite(grain, leaf, batch, new TaskCompletionSource<SplitResult?>()));
        Assert.That(ShardActivationRetry.IsTransientSiloChurn(refused!), Is.True);
        await leaf.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);

        gate.SetResult(null);
        await inFlight.WaitAsync(TimeSpan.FromSeconds(5));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Flush_suspension_waits_for_both_write_families_and_issues_deactivation_once(bool batchFirst)
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var pointGate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var batchGate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var point = InvokeGatedWrite(grain, leaf, false, pointGate);
        var batch = InvokeGatedWrite(grain, leaf, true, batchGate);
        await suspend();
        await grain.ForceDeactivateAsync();
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);

        (batchFirst ? batchGate : pointGate).SetResult(null);
        await (batchFirst ? batch : point).WaitAsync(TimeSpan.FromSeconds(5));
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        (batchFirst ? pointGate : batchGate).SetResult(null);
        await Task.WhenAll(point, batch).WaitAsync(TimeSpan.FromSeconds(5));
        await grain.ForceDeactivateAsync();
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task ForceDeactivateAsync_fences_and_defers_while_SetManyAsync_is_in_flight()
    {
        var (grain, leaf, context, _) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = InvokeGatedWrite(grain, leaf, true, gate);
        var filter = (IIncomingGrainCallFilter)grain;

        await filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.ForceDeactivateAsync),
            grain.ForceDeactivateAsync)).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(grain.DeactivationRequested, Is.True);
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        Assert.ThrowsAsync<ShardRootDeactivatingException>(() => grain.SetManyAsync([]));
        gate.SetResult(null);
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task ForceDeactivateAsync_fences_and_defers_while_SetManyWherePredicateAsync_is_in_flight()
    {
        var (grain, leaf, context, _) = await CreateDeactivationHarnessAsync();
        var predicate = LatticePredicateNode.Member("Score");
        var gate = new TaskCompletionSource<ConditionalSetManyResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.SetManyWherePredicateAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), predicate)
            .Returns(gate.Task);
        var filter = (IIncomingGrainCallFilter)grain;
        var method = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.SetManyWherePredicateAsync))!;
        var write = filter.Invoke(QuiesceCallContext(method.Name,
            () => grain.SetManyWherePredicateAsync([new("write", [1])], predicate), method));
        Assert.That(write.IsCompleted, Is.False);

        await filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.ForceDeactivateAsync),
            grain.ForceDeactivateAsync)).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(grain.DeactivationRequested, Is.True);
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        gate.SetResult(new ConditionalSetManyResult { WrittenKeys = ["write"] });
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task SetManyWherePredicateAsync_admitted_after_flush_suspension_is_refused_before_leaf_dispatch()
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = InvokeGatedWrite(grain, leaf, false, gate);
        await suspend();

        var predicate = LatticePredicateNode.Member("Score");
        var method = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.SetManyWherePredicateAsync))!;
        var refused = Assert.ThrowsAsync<ShardRootDeactivatingException>(() =>
            ((IIncomingGrainCallFilter)grain).Invoke(QuiesceCallContext(method.Name,
                () => grain.SetManyWherePredicateAsync([new("refused", [2])], predicate), method)));
        Assert.That(ShardActivationRetry.IsTransientSiloChurn(refused!), Is.True);
        await leaf.DidNotReceive().SetManyWherePredicateAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode>());
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);

        gate.SetResult(null);
        await inFlight.WaitAsync(TimeSpan.FromSeconds(5));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task Serial_turn_does_not_wait_for_in_flight_SetManyAsync()
    {
        var (grain, leaf, _, _) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = InvokeGatedWrite(grain, leaf, true, gate);
        var invoked = false;
        await ((IIncomingGrainCallFilter)grain).Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.EnterRejectPhaseAsync), () =>
            {
                invoked = true;
                return Task.CompletedTask;
            })).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(invoked, Is.True);
        gate.SetResult(null);
        await write;
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Faulted_write_still_completes_deferred_deactivation(bool batch)
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = InvokeGatedWrite(grain, leaf, batch, gate);
        await suspend();
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        gate.SetException(new InvalidOperationException("write failed"));
        Assert.ThrowsAsync<InvalidOperationException>(() => write.WaitAsync(TimeSpan.FromSeconds(5)));
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Runtime_refusal_after_drain_reopens_admission_without_failing_completed_write(bool batch)
    {
        var (grain, leaf, context, suspend) = await CreateDeactivationHarnessAsync();
        var refuse = true;
        context.When(c => c.Deactivate(Arg.Any<DeactivationReason>()))
            .Do(_ =>
            {
                if (refuse) throw new InvalidOperationException("no runtime");
            });
        var gate = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var write = InvokeGatedWrite(grain, leaf, batch, gate);
        await suspend();
        Assert.That(grain.DeactivationRequested, Is.True);
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        leaf.ClearReceivedCalls();
        var refused = Assert.ThrowsAsync<ShardRootDeactivatingException>(() => grain.SetManyAsync([new("refused", [2])]));
        Assert.That(ShardActivationRetry.IsTransientSiloChurn(refused!), Is.True);
        await leaf.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        gate.SetResult(null);
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(grain.DeactivationRequested, Is.False);

        await InvokeGatedWrite(grain, leaf, false, gate);
        await grain.SetManyAsync([]);
        refuse = false;
        await grain.ForceDeactivateAsync();
        Assert.That(grain.DeactivationRequested, Is.True);
        context.ReceivedWithAnyArgs(2).Deactivate(default!);
    }

    [Test]
    public async Task Runtime_refusal_without_in_flight_writes_leaves_activation_usable()
    {
        var (grain, _, context, suspend) = await CreateDeactivationHarnessAsync();
        context.When(c => c.Deactivate(Arg.Any<DeactivationReason>()))
            .Do(_ => throw new InvalidOperationException("no runtime"));
        await suspend();
        Assert.That(grain.DeactivationRequested, Is.False);
        await grain.SetManyAsync([]);
        Assert.ThrowsAsync<InvalidOperationException>(grain.ForceDeactivateAsync);
        Assert.That(grain.DeactivationRequested, Is.False);
    }
}
