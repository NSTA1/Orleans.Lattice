using System.Reflection;
using NSubstitute;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization.Invocation;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Point-write quiesce guard tests (issue #812). Interleaved point writes overlap
/// each other, but no serial shard-root turn may start while one is in flight, and
/// no point write may start while a serial turn is active.
/// </summary>
public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static IIncomingGrainCallContext QuiesceCallContext(
        string methodName, Func<Task> invoke, MethodInfo? method = null, Type? interfaceType = null)
    {
        var request = Substitute.For<IInvokable>();
        request.GetInterfaceType().Returns(interfaceType ?? typeof(IShardRootGrain));
        request.GetMethodName().Returns(methodName);
        request.GetMethod().Returns(method);

        var context = Substitute.For<IIncomingGrainCallContext>();
        context.Request.Returns(request);
        context.Invoke().Returns(_ => invoke());
        return context;
    }

    [Test]
    public void ClassifyIncomingTurn_matches_the_orleans_generated_invokables()
    {
        // The runtime filter sees Orleans' generated invokables, so classify those:
        // a codegen change in how the method is reported must not silently turn a
        // serial turn into a pass-through.
        var invokables = typeof(IShardRootGrain).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.ContainsGenericParameters && typeof(IInvokable).IsAssignableFrom(t))
            .Select(t => Activator.CreateInstance(t) as IInvokable)
            .Where(i => i is not null && i.GetInterfaceType() == typeof(IShardRootGrain))
            .Select(i => i!)
            .ToArray();

        Assert.That(invokables, Has.Length.GreaterThan(20));
        Assert.Multiple(() =>
        {
            foreach (var invokable in invokables)
            {
                var name = invokable.GetMethodName();
                var expected = name == nameof(IShardRootGrain.SetAsync)
                    ? ShardRootGrain.IncomingTurnKind.PointWrite
                    : invokable.GetMethod()!.IsDefined(typeof(AlwaysInterleaveAttribute), inherit: true)
                        ? ShardRootGrain.IncomingTurnKind.Interleaved
                        : ShardRootGrain.IncomingTurnKind.Serial;
                Assert.That(ShardRootGrain.ClassifyIncomingTurn(invokable), Is.EqualTo(expected), name);
            }
        });

        var kinds = invokables.Select(ShardRootGrain.ClassifyIncomingTurn).ToHashSet();
        Assert.That(kinds, Is.EquivalentTo(Enum.GetValues<ShardRootGrain.IncomingTurnKind>()),
            "The shard root must expose point writes, other interleaved calls, and serial turns.");
    }

    [TestCase(nameof(IShardRootGrain.EnterRejectPhaseAsync))]
    [TestCase(nameof(IShardRootGrain.CompleteSplitAsync))]
    [TestCase(nameof(IShardRootGrain.MarkLeavesMovedAwayAsync))]
    [TestCase(nameof(IShardRootGrain.GetAsync))]
    public void ClassifyIncomingTurn_treats_split_and_fold_transitions_and_serial_reads_as_serial(string methodName)
    {
        var method = typeof(IShardRootGrain).GetMethods().First(m => m.Name == methodName);
        var request = Substitute.For<IInvokable>();
        request.GetInterfaceType().Returns(typeof(IShardRootGrain));
        request.GetMethodName().Returns(methodName);
        request.GetMethod().Returns(method);

        Assert.That(ShardRootGrain.ClassifyIncomingTurn(request), Is.EqualTo(ShardRootGrain.IncomingTurnKind.Serial));
    }

    [Test]
    public void ClassifyIncomingTurn_treats_a_foreign_interface_without_the_attribute_as_serial()
    {
        var request = Substitute.For<IInvokable>();
        request.GetInterfaceType().Returns(typeof(IRemindable));
        request.GetMethodName().Returns(nameof(IRemindable.ReceiveReminder));
        request.GetMethod().Returns(typeof(IRemindable).GetMethod(nameof(IRemindable.ReceiveReminder)));

        Assert.That(ShardRootGrain.ClassifyIncomingTurn(request), Is.EqualTo(ShardRootGrain.IncomingTurnKind.Serial));
    }

    [Test]
    public void ClassifyIncomingTurn_treats_an_unresolvable_method_as_serial()
    {
        var request = Substitute.For<IInvokable>();
        request.GetInterfaceType().Returns(typeof(IShardRootGrain));
        request.GetMethodName().Returns("SomeFutureMethodAsync");
        request.GetMethod().Returns((MethodInfo?)null);

        Assert.That(ShardRootGrain.ClassifyIncomingTurn(request), Is.EqualTo(ShardRootGrain.IncomingTurnKind.Serial));
    }

    [Test]
    public async Task Serial_turn_waits_for_in_flight_point_writes_to_drain()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serialInvoked = false;

        var w1 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => first.Task));
        var w2 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => second.Task));
        Assert.That(grain.InterleavedPointWritesInFlight, Is.EqualTo(2), "Point writes overlap each other.");

        var serial = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.EnterRejectPhaseAsync), () =>
        {
            serialInvoked = true;
            return Task.CompletedTask;
        }));

        first.SetResult();
        await w1;
        Assert.That(serialInvoked, Is.False, "The serial turn must wait for every in-flight point write.");

        second.SetResult();
        await w2;
        await serial;
        Assert.That(serialInvoked, Is.True);
        Assert.That(grain.InterleavedPointWritesInFlight, Is.Zero);
    }

    [Test]
    public async Task Point_write_waits_while_a_serial_turn_is_active()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var serialBody = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writeInvoked = false;

        var serial = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.CompleteSplitAsync), () => serialBody.Task));
        var write = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () =>
        {
            writeInvoked = true;
            return Task.CompletedTask;
        }));

        Assert.That(writeInvoked, Is.False, "A point write must not start inside an active serial turn.");

        serialBody.SetResult();
        await serial;
        await write;
        Assert.That(writeInvoked, Is.True);
    }

    [Test]
    public async Task Point_write_queued_behind_a_draining_serial_turn_runs_after_it()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var order = new List<string>();

        var w1 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => inFlight.Task));
        var serial = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.EnterRejectPhaseAsync), () =>
        {
            order.Add("serial");
            return Task.CompletedTask;
        }));
        var w2 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () =>
        {
            order.Add("w2");
            return Task.CompletedTask;
        }));

        Assert.That(order, Is.Empty, "The later point write is held back once a serial turn is waiting.");

        inFlight.SetResult();
        await Task.WhenAll(w1, serial, w2);
        Assert.That(order, Is.EqualTo(new[] { "serial", "w2" }));
    }

    [Test]
    public async Task Other_interleaved_calls_pass_through_while_a_serial_turn_drains()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var setMany = typeof(IShardRootGrain).GetMethods().First(m =>
            m.Name == nameof(IShardRootGrain.SetManyAsync) && m.IsDefined(typeof(AlwaysInterleaveAttribute)));
        var setManyInvoked = false;

        var w1 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => inFlight.Task));
        var serial = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.EnterRejectPhaseAsync), () => Task.CompletedTask));
        await filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetManyAsync), () =>
        {
            setManyInvoked = true;
            return Task.CompletedTask;
        }, setMany));

        Assert.That(setManyInvoked, Is.True,
            "A point write's leaf and split callbacks arrive as other interleaved calls; holding them back would deadlock.");

        inFlight.SetResult();
        await Task.WhenAll(w1, serial);
    }

    [Test]
    public async Task Faulted_point_write_still_releases_a_waiting_serial_turn()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serialInvoked = false;

        var write = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => inFlight.Task));
        var serial = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.EnterRejectPhaseAsync), () =>
        {
            serialInvoked = true;
            return Task.CompletedTask;
        }));

        inFlight.SetException(new InvalidOperationException("leaf write failed"));
        Assert.ThrowsAsync<InvalidOperationException>(() => write);
        await serial;

        Assert.That(serialInvoked, Is.True);
        Assert.That(grain.InterleavedPointWritesInFlight, Is.Zero);
    }

    [Test]
    public async Task Synchronously_throwing_serial_turn_releases_held_point_writes()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var writeInvoked = false;

        Assert.Throws<InvalidOperationException>(() => filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.CompleteSplitAsync), () => throw new InvalidOperationException("sync fault"))));

        await filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () =>
        {
            writeInvoked = true;
            return Task.CompletedTask;
        }));
        Assert.That(writeInvoked, Is.True, "A serial turn that throws synchronously must not leave point writes held back.");
    }

    [Test]
    public async Task Point_write_held_behind_ForceDeactivateAsync_is_refused_without_dispatching()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var heldInvoked = false;

        var w1 = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => inFlight.Task));
        var deactivate = filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.ForceDeactivateAsync), () => grain.ForceDeactivateAsync()));
        var held = filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () =>
        {
            heldInvoked = true;
            return Task.CompletedTask;
        }));

        Assert.That(grain.DeactivationRequested, Is.False, "Deactivation waits for the in-flight point write.");

        inFlight.SetResult();
        await w1;
        await deactivate;

        Assert.That(grain.DeactivationRequested, Is.True);
        Assert.ThrowsAsync<ShardRootDeactivatingException>(() => held);
        Assert.That(heldInvoked, Is.False,
            "A point write dispatched into a deactivating activation would stall on the leaf's footprint callback.");
        Assert.That(grain.InterleavedPointWritesInFlight, Is.Zero);
    }

    [Test]
    public async Task Point_write_arriving_after_deactivation_was_requested_is_refused()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var invoked = false;

        await filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.ForceDeactivateAsync), () => grain.ForceDeactivateAsync()));

        var ex = Assert.ThrowsAsync<ShardRootDeactivatingException>(() => filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.SetAsync), () =>
            {
                invoked = true;
                return Task.CompletedTask;
            })));

        Assert.Multiple(() =>
        {
            Assert.That(invoked, Is.False);
            Assert.That(ex!.ShardKey, Does.Contain(ShardKey));
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(ex), Is.True, "Callers must retry it.");
        });
    }

    [Test]
    public async Task Other_calls_still_run_after_deactivation_was_requested()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var serialInvoked = false;

        await filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.ForceDeactivateAsync), () => grain.ForceDeactivateAsync()));
        await filter.Invoke(QuiesceCallContext(nameof(IShardRootGrain.CompleteSplitAsync), () =>
        {
            serialInvoked = true;
            return Task.CompletedTask;
        }));

        Assert.That(serialInvoked, Is.True, "Only point and batch writes are fenced; split and fold work must still complete.");
    }
}