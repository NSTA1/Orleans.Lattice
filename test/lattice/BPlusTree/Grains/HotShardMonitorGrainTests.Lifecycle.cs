using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the autonomic monitor's activation lifecycle - the keepalive
/// reminder tick, the teardown path, and the sampling timer's fault envelope -
/// plus the two resilience arms the sampling pass itself owns: an occupancy
/// probe that fails to dispatch, and a split coordinator that refuses a trigger
/// because it is already busy.
/// <para>
/// The main fixture deliberately wires no <see cref="ITimerRegistry"/>, so
/// <c>StartTimer</c> throws there and everything downstream of it (the tick
/// callback, <c>StopAsync</c>'s timer disposal, and the reminder-driven timer
/// re-arm) is unreachable. This partial wires one, which is what makes the
/// lifecycle observable without sleeping on a real timer.
/// </para>
/// </summary>
public partial class HotShardMonitorGrainTests
{
    private sealed record LifecycleHarness(
        HotShardMonitorGrain Grain,
        IGrainFactory GrainFactory,
        ILattice Lattice,
        ITreeShardSplitGrain SplitGrain,
        IReminderRegistry Reminders,
        ITimerRegistry Timers,
        Func<int, IShardRootGrain> ShardOf);

    /// <summary>
    /// Builds a monitor whose grain context can resolve an
    /// <see cref="ITimerRegistry"/>, so the sampling timer arms for real and its
    /// callback can be captured and fired deterministically.
    /// </summary>
    private static LifecycleHarness CreateLifecycleGrain(
        int physicalShardCount = 2,
        int virtualShardCount = 16,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("monitor", TreeId));

        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timers);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminders = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 1,
        };
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var lattice = Substitute.For<ILattice>();
        lattice.IsResizeCompleteAsync().Returns(true);
        lattice.IsReshardCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);
        lattice.IsSnapshotCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var splitGrain = Substitute.For<ITreeShardSplitGrain>();
        splitGrain.IsIdleAsync().Returns(true);
        grainFactory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(splitGrain);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(ShardMap.CreateDefault(virtualShardCount, physicalShardCount));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = physicalShardCount,
            }));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);

        var shardSubs = new Dictionary<int, IShardRootGrain>();
        IShardRootGrain Shard(int i)
        {
            if (shardSubs.TryGetValue(i, out var s)) return s;
            var sub = Substitute.For<IShardRootGrain>();
            sub.GetHotnessAsync().Returns(new ShardHotness { Reads = 0, Writes = 0, Window = TimeSpan.FromSeconds(30) });
            sub.HasPendingBulkOperationAsync().Returns(false);
            sub.IsSplittingAsync().Returns(false);
            sub.CountAsync().Returns(WellOccupiedShardEntryCount);
            shardSubs[i] = sub;
            return sub;
        }
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return Shard(idx);
        });

        var grain = new HotShardMonitorGrain(
            context, grainFactory, reminders, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<HotShardMonitorGrain>(),
            new FakePersistentState<HotShardMonitorState>());

        return new LifecycleHarness(grain, grainFactory, lattice, splitGrain, reminders, timers, Shard);
    }

    /// <summary>The sampling tick the grain handed to the timer registry.</summary>
    private static Func<CancellationToken, Task> CapturedSamplingTick(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private static int TimersRegistered(ITimerRegistry registry) =>
        registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    private static void MakeHot(IShardRootGrain shard, int reads = 10_000) =>
        shard.GetHotnessAsync().Returns(new ShardHotness
        {
            Reads = reads,
            Writes = 0,
            Window = TimeSpan.FromSeconds(10),
        });

    // ---------------------------------------------------------------- StopAsync

    [Test]
    public async Task StopAsync_unregisters_the_keepalive_reminder_and_clears_the_running_flag()
    {
        var h = CreateLifecycleGrain();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>()).Returns(reminder);

        await h.Grain.EnsureRunningAsync();
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1), "EnsureRunningAsync should arm the sampling timer");

        await h.Grain.StopAsync();

        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);

        // A stopped monitor is genuinely stopped: EnsureRunningAsync must arm a
        // second timer rather than short-circuit on a stale _running flag.
        await h.Grain.EnsureRunningAsync();
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(2),
            "StopAsync must clear _running so a later EnsureRunningAsync re-arms the timer");
    }

    [Test]
    public async Task StopAsync_unregisters_nothing_when_no_keepalive_reminder_is_registered()
    {
        var h = CreateLifecycleGrain();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await h.Grain.EnsureRunningAsync();
        await h.Grain.StopAsync();

        await h.Reminders.Received(1).GetReminder(Arg.Any<GrainId>(), Arg.Any<string>());
        await h.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task StopAsync_swallows_a_reminder_registry_failure_and_still_stops()
    {
        // Teardown is best effort: a reminder table blip must not leave the
        // monitor half-stopped with its sampling timer still armed, because the
        // tick would keep triggering splits for a tree the operator disabled.
        var h = CreateLifecycleGrain();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns<Task<IGrainReminder?>>(_ => throw new InvalidOperationException("reminder table unavailable"));

        await h.Grain.EnsureRunningAsync();

        Assert.DoesNotThrowAsync(() => h.Grain.StopAsync());

        await h.Grain.EnsureRunningAsync();
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(2),
            "a failed reminder unregister must not prevent the monitor from being stopped");
    }

    // ----------------------------------------------------------- ReceiveReminder

    [Test]
    public async Task ReceiveReminder_ignores_a_reminder_it_does_not_own()
    {
        var h = CreateLifecycleGrain();

        await h.Grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(TimersRegistered(h.Timers), Is.Zero, "a foreign reminder must not arm the sampling timer");
        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_does_nothing_while_auto_split_is_disabled()
    {
        // The keepalive outlives an operator turning auto-split off, so the tick
        // must not resurrect the sampling timer for a tree that opted out.
        var h = CreateLifecycleGrain(options: new LatticeOptions { AutoSplitEnabled = false });

        await h.Grain.ReceiveReminder("hot-shard-monitor", new TickStatus());

        Assert.That(TimersRegistered(h.Timers), Is.Zero);
        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_re_registers_a_keepalive_whose_period_has_drifted()
    {
        var h = CreateLifecycleGrain();

        // A default TickStatus reports a zero period, which is exactly the
        // stale-period shape the defensive re-registration exists to correct.
        await h.Grain.ReceiveReminder("hot-shard-monitor", new TickStatus());

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "hot-shard-monitor",
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1),
            "the keepalive tick must re-arm the sampling timer on an activation that lost it");
    }

    [Test]
    public async Task ReceiveReminder_leaves_a_correctly_periodic_keepalive_alone()
    {
        var h = CreateLifecycleGrain();
        var onPeriod = new TickStatus(DateTime.UtcNow, TimeSpan.FromMinutes(1), DateTime.UtcNow);

        await h.Grain.ReceiveReminder("hot-shard-monitor", onPeriod);

        await h.Reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1));
    }

    [Test]
    public async Task ReceiveReminder_does_not_arm_a_second_timer_when_one_is_already_running()
    {
        // The keepalive fires every minute for the whole life of the activation,
        // so re-arming on every tick would leak a sampling timer per minute.
        var h = CreateLifecycleGrain();
        await h.Grain.EnsureRunningAsync();
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1));

        var onPeriod = new TickStatus(DateTime.UtcNow, TimeSpan.FromMinutes(1), DateTime.UtcNow);
        await h.Grain.ReceiveReminder("hot-shard-monitor", onPeriod);
        await h.Grain.ReceiveReminder("hot-shard-monitor", onPeriod);

        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1),
            "an already-armed sampling timer must be left alone by the keepalive tick");
    }

    // ------------------------------------------------------- sampling tick fault

    [Test]
    public async Task A_sampling_tick_runs_the_pass_it_was_armed_for()
    {
        // The armed callback must be the sampling pass itself, not a wrapper that
        // silently does nothing: a timer that ticks without sampling would leave
        // auto-split permanently dormant while looking healthy.
        var h = CreateLifecycleGrain();
        await h.Grain.EnsureRunningAsync();
        var tick = CapturedSamplingTick(h.Timers);

        MakeHot(h.ShardOf(1));

        await tick(CancellationToken.None);

        await h.SplitGrain.Received(1).SplitAsync(1);
    }

    [Test]
    public async Task A_sampling_tick_swallows_a_failing_pass_so_the_timer_keeps_ticking()
    {
        // The tick is the only caller of the sampling pass. If it let a fault
        // escape, Orleans would tear the timer down and auto-split would stay
        // silently dead for the rest of the activation.
        var h = CreateLifecycleGrain();
        await h.Grain.EnsureRunningAsync();
        var tick = CapturedSamplingTick(h.Timers);

        MakeHot(h.ShardOf(1));
        // A synchronous dispatch failure on the occupancy probe, which the pass
        // deliberately rethrows rather than absorbing.
        h.ShardOf(1).CountAsync().Returns<Task<int>>(_ => throw new InvalidOperationException("shard unreachable"));

        Assert.DoesNotThrowAsync(() => tick(CancellationToken.None));

        await h.SplitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public void RunSamplingPass_surfaces_a_synchronous_occupancy_probe_dispatch_failure()
    {
        // The occupancy floor captures a synchronous dispatch fault and rethrows
        // it with its original stack rather than admitting an unmeasured shard,
        // which would let a nearly empty shard split and double its footprint.
        var h = CreateLifecycleGrain();
        MakeHot(h.ShardOf(1));
        h.ShardOf(1).CountAsync().Returns<Task<int>>(_ => throw new InvalidOperationException("shard unreachable"));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RunSamplingPassAsync());

        Assert.That(ex!.Message, Is.EqualTo("shard unreachable"));
    }

    [Test]
    public async Task A_busy_split_coordinator_is_ignored_until_the_next_tick()
    {
        // The coordinator refuses a trigger while it is mid-split on a different
        // parameter set. That is routine contention, not a pass failure, so the
        // shard must simply stay uncooled and be retried on the next tick.
        var h = CreateLifecycleGrain();
        MakeHot(h.ShardOf(1));
        h.SplitGrain.SplitAsync(Arg.Any<int>())
            .Returns(Task.FromException(new InvalidOperationException("split coordinator busy")));

        Assert.DoesNotThrowAsync(() => h.Grain.RunSamplingPassAsync());
        await h.SplitGrain.Received(1).SplitAsync(1);

        // No cooldown was recorded for the refused shard, so the very next pass
        // retries it rather than waiting out a cooldown it never earned.
        h.SplitGrain.ClearReceivedCalls();
        h.SplitGrain.SplitAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        await h.Grain.RunSamplingPassAsync();
        await h.SplitGrain.Received(1).SplitAsync(1);
    }

    // ------------------------------------------------- outstanding footprint refresh

    [Test]
    public async Task A_suppressed_pass_refreshes_an_outstanding_footprint_through_the_cluster_gate()
    {
        // A suppressor ends the pass before it can recompute the in-flight count,
        // but the splits it already started keep draining. Without this refresh
        // the footprint would lapse and the scale-in gate would see an idle
        // cluster while splits were genuinely in flight.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
            MaxClusterConcurrentAutoSplits = 4,
        };
        var h = CreateLifecycleGrain(options: opts);
        var gate = SubstituteGate(h.GrainFactory, grant: 1);
        MakeHot(h.ShardOf(1));

        // Pass one publishes a footprint of 1 through the admission gate.
        await h.Grain.RunSamplingPassAsync();
        await h.SplitGrain.Received(1).SplitAsync(1);
        gate.ClearReceivedCalls();

        // Pass two is suppressed by an in-flight resize.
        h.Lattice.IsResizeCompleteAsync().Returns(false);
        await h.Grain.RunSamplingPassAsync();

        await gate.Received(1).AcquireSlotsAsync(
            TreeId,
            1,
            0,
            4,
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_footprint_refresh_swallows_a_cluster_gate_failure()
    {
        // The refresh is pure observability sitting upstream of the split
        // triggers, so a gate blip must cost at most one lapsed footprint and
        // never abort the pass.
        var opts = new LatticeOptions
        {
            AutoSplitMinTreeAge = TimeSpan.Zero,
            HotShardOpsPerSecondThreshold = 100,
            MaxConcurrentAutoSplits = 2,
            MaxClusterConcurrentAutoSplits = 4,
        };
        var h = CreateLifecycleGrain(options: opts);
        var gate = SubstituteGate(h.GrainFactory, grant: 1);
        MakeHot(h.ShardOf(1));

        await h.Grain.RunSamplingPassAsync();
        gate.ClearReceivedCalls();

        gate.AcquireSlotsAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<TimeSpan>())
            .Returns<int>(_ => throw new TimeoutException("cluster gate unavailable"));
        h.Lattice.IsMergeCompleteAsync().Returns(false);

        Assert.DoesNotThrowAsync(() => h.Grain.RunSamplingPassAsync());

        await gate.Received(1).AcquireSlotsAsync(
            TreeId, 1, 0, 4, Arg.Any<TimeSpan>());
    }
}
