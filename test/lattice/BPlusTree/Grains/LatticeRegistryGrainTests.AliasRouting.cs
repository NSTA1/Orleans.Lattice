using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task SetAliasAsync_waits_for_every_source_fence_before_publishing()
    {
        var (grain, rows, sources, _, observer) = ArrangeAliasMove();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        sources[1].MarkRetainedRedirectAsync("target", Arg.Any<string>(), "logical").Returns(gate.Task);

        var move = grain.SetAliasAsync("logical", "target");
        Assert.That(move.IsCompleted, Is.False);
        Assert.That(rows["logical"].PhysicalTreeId, Is.Null, "the alias is unchanged while a fence is not durable");
        Assert.That(observer.Changes, Is.Empty);
        gate.SetResult();
        await move;
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(rows["logical"].ShardMap!.Slots, Is.EqualTo(rows["target"].ShardMap!.Slots));
        Assert.That(observer.Changes, Has.Count.EqualTo(1));
    }

    [TestCase("fence")]
    [TestCase("publish")]
    public async Task SetAliasAsync_failure_rolls_back_routing_and_partial_fences_then_retry_succeeds(string failureStage)
    {
        var (grain, rows, sources, targets, observer) = ArrangeAliasMove(failureStage);
        var before = rows["logical"];
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));

        Assert.That(JsonSerializer.Serialize(rows["logical"]), Is.EqualTo(JsonSerializer.Serialize(before)),
            "restore the complete original routing pair");
        Assert.That(observer.Changes, Is.Empty, "a rolled-back move is not a committed alias event");
        foreach (var source in sources)
            await source.Received(1).ClearRetainedRedirectIfOwnedAsync(Arg.Is<string>(id => id.StartsWith("alias:logical->target:")));

        await grain.SetAliasAsync("logical", "target");
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(observer.Changes, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task SetAliasAsync_ambiguous_ack_completes_forward_preserving_destination_writes()
    {
        var destinationData = new Dictionary<string, byte[]>();
        IShardRootGrain destinationShard = null!;
        var (grain, rows, sources, targets, _) = ArrangeAliasMove("acknowledge", onPublished: async published =>
        {
            // Models a fresh router resolving the published row while the
            // mutator has not yet received the storage acknowledgement.
            Assert.That(published["logical"].PhysicalTreeId, Is.EqualTo("target"));
            await destinationShard.SetAsync("accepted", [42]);
        });
        destinationShard = targets[0];
        destinationShard.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(call =>
        {
            destinationData[call.Arg<string>()] = call.Arg<byte[]>();
            return Task.CompletedTask;
        });
        destinationShard.GetAsync("accepted").Returns(_ => Task.FromResult<byte[]?>(destinationData.GetValueOrDefault("accepted")));

        await grain.SetAliasAsync("logical", "target");

        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(await grain.ResolveAsync("logical"), Is.EqualTo("target"));
        Assert.That(await destinationShard.GetAsync("accepted"), Is.EqualTo(new byte[] { 42 }));
        foreach (var source in sources)
            await source.DidNotReceive().ClearRetainedRedirectIfOwnedAsync(Arg.Any<string>());
        foreach (var target in targets)
            await target.DidNotReceive().MarkRetainedRedirectAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task SetAliasAsync_ambiguous_ack_cannot_overwrite_a_newer_alias_publication()
    {
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>();
        TreeRegistryEntry? newer = null;
        var (grain, rows, sources, targets, _) = ArrangeAliasMove("acknowledge", pending, published =>
        {
            newer = published["logical"] with { PhysicalTreeId = "newer", AliasRoutingOperationId = "newer-operation" };
            published["logical"] = newer;
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));
        await grain.ReceiveReminder("alias-routing-recovery", default);
        Assert.That(rows["logical"], Is.SameAs(newer));
        Assert.That(pending.State, Is.Empty);
        foreach (var source in sources)
            await source.DidNotReceive().ClearRetainedRedirectIfOwnedAsync(Arg.Any<string>());
        foreach (var target in targets)
            await target.DidNotReceive().MarkRetainedRedirectAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task ReceiveReminder_rehydrates_and_finishes_committed_move_without_caller_retry()
    {
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>();
        IGrainFactory factory = null!;
        IOptionsMonitor<LatticeOptions> options = null!;
        var (grain, rows, _, targets, _) = ArrangeAliasMove("release", pending,
            captureDependencies: (f, o) => { factory = f; options = o; });
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(pending.State, Has.Count.EqualTo(1));

        var durable = JsonSerializer.Deserialize<Dictionary<string, AliasRoutingMoveState>>(JsonSerializer.Serialize(pending.State))!;
        pending.State = durable;
        // A new POCO activation uses only durable state, not the failed caller.
        var reactivated = new LatticeRegistryGrain(
            factory, options, aliasRoutingState: pending);
        await reactivated.ReceiveReminder("alias-routing-recovery", default);
        Assert.That(pending.State, Is.Empty);
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        foreach (var target in targets)
            await target.Received().ReleaseRetainedRedirectAsync("logical");
        await targets[1].Received(2).ReleaseRetainedRedirectAsync("logical");
    }

    [Test]
    public async Task ReceiveReminder_rehydrates_prepublication_intent_and_moves_forward_without_caller_retry()
    {
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>();
        string? checkpoint = null;
        pending.OnWriteState = state => checkpoint ??= JsonSerializer.Serialize(state);
        IGrainFactory factory = null!;
        IOptionsMonitor<LatticeOptions> options = null!;
        var (grain, rows, _, _, _) = ArrangeAliasMove("fence", pending,
            captureDependencies: (f, o) => { factory = f; options = o; });
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));
        Assert.That(rows["logical"].PhysicalTreeId, Is.Null);
        pending.State = JsonSerializer.Deserialize<Dictionary<string, AliasRoutingMoveState>>(checkpoint!)!;
        var reactivated = new LatticeRegistryGrain(factory, options, aliasRoutingState: pending);
        await reactivated.ReceiveReminder("alias-routing-recovery", default);
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(pending.State, Is.Empty);
    }

    [Test]
    public async Task SetAliasAsync_intent_persist_failure_installs_no_source_fences()
    {
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>
        {
            ThrowOnWrite = new IOException("intent persistence refused"),
        };
        var (grain, rows, sources, _, _) = ArrangeAliasMove(pending: pending);
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));
        Assert.That(rows["logical"].PhysicalTreeId, Is.Null);
        foreach (var source in sources)
            await source.DidNotReceive().MarkRetainedRedirectAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
        await grain.ReceiveReminder("alias-routing-recovery", default);
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
    }

    [Test]
    public async Task ReceiveReminder_recovers_failed_partial_fence_rollback_and_keeps_newer_configuration()
    {
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>();
        var (grain, rows, sources, _, _) = ArrangeAliasMove("fence", pending);
        var failRollback = true;
        sources[0].ClearRetainedRedirectIfOwnedAsync(Arg.Any<string>()).Returns(_ =>
        {
            if (failRollback)
            {
                failRollback = false;
                throw new IOException("rollback persist failed");
            }
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<AggregateException>(() => grain.SetAliasAsync("logical", "target"));
        Assert.That(pending.State, Has.Count.EqualTo(1));
        rows["logical"] = rows["logical"] with { MaxLeafKeys = 731 };
        await grain.ReceiveReminder("alias-routing-recovery", default);
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(rows["logical"].MaxLeafKeys, Is.EqualTo(731));
        Assert.That(pending.State, Is.Empty);
    }

    [Test]
    public async Task Alias_recovery_registers_reminder_before_intent_and_schedules_exclusive_post_activation_tick()
    {
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(), Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        using var services = new ServiceCollection().AddSingleton(timers).BuildServiceProvider();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("registry-test", LatticeConstants.RegistryTreeId));
        context.ActivationServices.Returns(services);
        var reminders = Substitute.For<IReminderRegistry>();
        var pending = new FakePersistentState<Dictionary<string, AliasRoutingMoveState>>();
        pending.BeforeWrite = async () => await reminders.Received(1).RegisterOrUpdateReminder(
            context.GrainId, "alias-routing-recovery", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        IGrainFactory factory = null!;
        IOptionsMonitor<LatticeOptions> options = null!;
        var (grain, rows, _, targets, _) = ArrangeAliasMove("release", pending,
            captureDependencies: (f, o) => { factory = f; options = o; },
            context: context, reminders: reminders);
        Assert.ThrowsAsync<IOException>(() => grain.SetAliasAsync("logical", "target"));
        await ((IGrainBase)grain).OnDeactivateAsync(default, CancellationToken.None);
        var reactivated = new LatticeRegistryGrain(factory, options, context: context,
            reminderRegistry: reminders, aliasRoutingState: pending);
        var releaseCalls = targets[1].ReceivedCalls().Count();
        await ((IGrainBase)reactivated).OnActivateAsync(CancellationToken.None);
        Assert.That(targets[1].ReceivedCalls().Count(), Is.EqualTo(releaseCalls),
            "activation schedules, but does not await fan-out to shards which read the registry");
        var registration = timers.ReceivedCalls().Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        var timerOptions = (GrainTimerCreationOptions)registration.GetArguments()[3]!;
        Assert.That(timerOptions.Interleave, Is.False);
        await ((Func<CancellationToken, Task>)registration.GetArguments()[2]!)(CancellationToken.None);
        Assert.That(rows["logical"].PhysicalTreeId, Is.EqualTo("target"));
        Assert.That(pending.State, Is.Empty);
    }

    [Test]
    public async Task SwapAliasAsync_stale_expected_target_does_not_install_any_redirect()
    {
        var (grain, rows, sources, targets, _) = ArrangeAliasMove();
        var before = rows["logical"];
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            grain.SwapAliasAsync("logical", "target", rows["target"].ShardMap!, null, "stale-copy"));
        Assert.That(rows["logical"], Is.EqualTo(before));
        foreach (var shard in sources.Concat(targets))
            await shard.DidNotReceive().MarkRetainedRedirectAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    private static (LatticeRegistryGrain Grain, Dictionary<string, TreeRegistryEntry> Rows,
        IShardRootGrain[] Sources, IShardRootGrain[] Targets, RecordingTreeAliasObserver Observer)
        ArrangeAliasMove(string? failureStage = null,
            FakePersistentState<Dictionary<string, AliasRoutingMoveState>>? pending = null,
            Func<Dictionary<string, TreeRegistryEntry>, Task>? onPublished = null,
            Action<IGrainFactory, IOptionsMonitor<LatticeOptions>>? captureDependencies = null,
            IGrainContext? context = null, IReminderRegistry? reminders = null)
    {
        var rows = new Dictionary<string, TreeRegistryEntry>
        {
            ["logical"] = new()
            {
                ShardCount = 2,
                ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2),
                Lineage = Guid.NewGuid(),
            },
            ["target"] = new()
            {
                ShardCount = 3,
                ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 3),
                NextShardIndex = 7,
            },
        };
        var fail = true;
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ISystemLattice>();
        factory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(tree);
        tree.GetAsync(Arg.Any<string>()).Returns(call =>
            Task.FromResult(rows.TryGetValue(call.Arg<string>(), out var row) ? JsonSerializer.SerializeToUtf8Bytes(row) : null));
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(async call =>
        {
            if (fail && failureStage == "publish")
            {
                fail = false;
                throw new IOException("registry write refused");
            }
            rows[call.Arg<string>()] = JsonSerializer.Deserialize<TreeRegistryEntry>(call.Arg<byte[]>())!;
            if (fail && failureStage == "acknowledge")
            {
                fail = false;
                if (onPublished is not null) await onPublished(rows);
                throw new IOException("registry acknowledgement lost after commit");
            }
        });

        var sources = Enumerable.Range(0, 2).Select(_ => Substitute.For<IShardRootGrain>()).ToArray();
        var targets = Enumerable.Range(0, 3).Select(_ => Substitute.For<IShardRootGrain>()).ToArray();
        for (var i = 0; i < sources.Length; i++)
            factory.GetGrain<IShardRootGrain>($"logical/{i}", null).Returns(sources[i]);
        for (var i = 0; i < targets.Length; i++)
            factory.GetGrain<IShardRootGrain>($"target/{i}", null).Returns(targets[i]);
        sources[1].MarkRetainedRedirectAsync("target", Arg.Any<string>(), "logical").Returns(_ =>
        {
            if (fail && failureStage == "fence")
            {
                fail = false;
                throw new IOException("one source fence failed");
            }
            return Task.CompletedTask;
        });
        targets[1].ReleaseRetainedRedirectAsync("logical").Returns(_ =>
        {
            if (fail && failureStage == "release")
            {
                fail = false;
                throw new IOException("one destination release failed");
            }
            return Task.CompletedTask;
        });

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        captureDependencies?.Invoke(factory, options);
        var observer = new RecordingTreeAliasObserver();
        var dispatcher = new TreeAliasObserverDispatcher([observer], NullLogger<TreeAliasObserverDispatcher>.Instance);
        return (new LatticeRegistryGrain(factory, options, aliasObservers: dispatcher,
            context: context, reminderRegistry: reminders, aliasRoutingState: pending), rows, sources, targets, observer);
    }
}
