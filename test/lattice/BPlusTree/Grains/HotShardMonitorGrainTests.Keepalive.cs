using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Keepalive-registration coverage (issue #3713, the hot-shard-monitor twin of
/// #3682): a keepalive reminder that cannot be registered, or an activation-time
/// write that faults, must never leave the monitor latched as running with no
/// sampling timer.
/// <para>
/// Orleans' reminder service initializes after the silo is active, and a
/// registration inside that window waits ~20s and then throws "Reminder Service
/// is still initializing". The monitor used to latch <c>_running</c>, await the
/// registration and the activation-time write, and only then start its timer,
/// so either fault left it claiming to run while nothing sampled. Every later
/// <c>EnsureRunningAsync</c> returned at the latch, so the tree never
/// auto-split for the life of the activation.
/// </para>
/// </summary>
public partial class HotShardMonitorGrainTests
{
    private const string KeepaliveName = "hot-shard-monitor";

    private static OrleansException ReminderServiceStillInitializing() => new(
        "Reminder Service is still initializing and it is taking a long time. Please retry again later.",
        new TimeoutException());

    private static Task<IGrainReminder> KeepaliveRegistration(LifecycleHarness h) =>
        h.Reminders.RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), KeepaliveName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

    private static Task KeepaliveRegistrations(LifecycleHarness h, int count) =>
        h.Reminders.Received(count).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), KeepaliveName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

    [Test]
    public async Task EnsureRunning_arms_the_sampling_timer_and_registers_the_keepalive()
    {
        var h = CreateLifecycleGrain();

        await h.Grain.EnsureRunningAsync();

        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1));
        await KeepaliveRegistrations(h, 1);
    }

    [Test]
    public async Task EnsureRunning_arms_the_sampling_timer_even_while_the_reminder_service_is_still_initializing()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).ThrowsAsync(ReminderServiceStillInitializing());

        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.Nothing,
            "the timer is armed, so the caller's request to ensure the monitor is running has been met");
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1),
            "a keepalive that could not yet be registered must not cost this activation its sampling timer");
        await Task.CompletedTask;
    }

    [Test]
    public async Task EnsureRunning_still_initializes_the_activation_time_after_a_deferred_keepalive()
    {
        var state = new FakePersistentState<HotShardMonitorState>();
        var h = CreateLifecycleGrain(state: state);
        KeepaliveRegistration(h).ThrowsAsync(ReminderServiceStillInitializing());

        await h.Grain.EnsureRunningAsync();

        Assert.That(state.State.ActivationUtc, Is.Not.Null,
            "a deferred keepalive is not a failed call, so the rest of the arming must still run");
    }

    [Test]
    public async Task EnsureRunning_surfaces_a_non_transient_reminder_fault_but_keeps_the_timer_armed()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).ThrowsAsync(new InvalidOperationException("reminder table down"));

        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.InvalidOperationException);
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1));
        await Task.CompletedTask;
    }

    [Test]
    public async Task EnsureRunning_keeps_the_timer_armed_when_the_activation_time_write_faults()
    {
        var state = new FakePersistentState<HotShardMonitorState>
        {
            ThrowOnWrite = new InvalidOperationException("simulated storage failure"),
        };
        var h = CreateLifecycleGrain(state: state);

        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.InvalidOperationException);
        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1),
            "the latch is already set, so a faulting write must not leave the monitor with no timer");
        await Task.CompletedTask;
    }

    [Test]
    public async Task EnsureRunning_does_not_arm_a_second_timer_after_a_failed_registration()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).ThrowsAsync(ReminderServiceStillInitializing());

        await h.Grain.EnsureRunningAsync();
        await h.Grain.EnsureRunningAsync();

        Assert.That(TimersRegistered(h.Timers), Is.EqualTo(1));
        await KeepaliveRegistrations(h, 1);
    }

    [Test]
    public async Task EnsureRunning_latches_without_arming_or_registering_while_auto_split_is_disabled()
    {
        // The documented asymmetry with the shard-healing orchestrator: this
        // monitor latches before the AutoSplitEnabled check (#2181), so a
        // disabled monitor arms nothing and never re-evaluates.
        var h = CreateLifecycleGrain(options: new LatticeOptions { AutoSplitEnabled = false });

        await h.Grain.EnsureRunningAsync();

        Assert.That(TimersRegistered(h.Timers), Is.Zero);
        await KeepaliveRegistrations(h, 0);
    }

    [Test]
    public async Task A_sampling_tick_retries_a_keepalive_registration_that_failed_while_initializing()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw ReminderServiceStillInitializing(),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await KeepaliveRegistrations(h, 2);
    }

    [Test]
    public async Task A_sampling_tick_retries_a_keepalive_registration_that_failed_non_transiently()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw new InvalidOperationException("reminder table down"),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.InvalidOperationException);

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await KeepaliveRegistrations(h, 2);
    }

    [Test]
    public async Task A_sampling_tick_swallows_a_keepalive_retry_that_faults_non_transiently()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw ReminderServiceStillInitializing(),
            _ => throw new InvalidOperationException("reminder table down"),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();
        var tick = CapturedSamplingTick(h.Timers);

        Assert.DoesNotThrowAsync(() => tick(CancellationToken.None),
            "a fault escaping the tick would tear the timer down");
        await tick(CancellationToken.None);

        await KeepaliveRegistrations(h, 3);
    }

    [Test]
    public async Task A_sampling_tick_stops_retrying_once_the_keepalive_is_registered()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw ReminderServiceStillInitializing(),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();
        var tick = CapturedSamplingTick(h.Timers);

        await tick(CancellationToken.None);
        await tick(CancellationToken.None);

        await KeepaliveRegistrations(h, 2);
    }

    [Test]
    public async Task A_sampling_tick_does_not_re_register_a_keepalive_that_already_succeeded()
    {
        var h = CreateLifecycleGrain();
        await h.Grain.EnsureRunningAsync();

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await KeepaliveRegistrations(h, 1);
    }

    [Test]
    public async Task A_sampling_tick_does_not_register_a_keepalive_that_just_fired()
    {
        // A firing keepalive proves it is registered, so the tick it re-armed
        // must not register it again.
        var h = CreateLifecycleGrain();
        var onPeriod = new TickStatus(DateTime.UtcNow, TimeSpan.FromMinutes(1), DateTime.UtcNow);
        await h.Grain.ReceiveReminder(KeepaliveName, onPeriod);

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await KeepaliveRegistrations(h, 0);
    }

    [Test]
    public async Task A_sampling_tick_retries_a_failed_keepalive_after_a_stop_and_restart()
    {
        // StopAsync unregisters the keepalive, so a restart must not inherit the
        // previous run's "registered" verdict.
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).Returns(
            _ => Task.FromResult(Substitute.For<IGrainReminder>()),
            _ => throw ReminderServiceStillInitializing(),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();
        await h.Grain.StopAsync();
        await h.Grain.EnsureRunningAsync();

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await KeepaliveRegistrations(h, 3);
    }

    [Test]
    public async Task A_sampling_tick_still_samples_while_the_keepalive_registration_keeps_failing()
    {
        var h = CreateLifecycleGrain();
        KeepaliveRegistration(h).ThrowsAsync(ReminderServiceStillInitializing());
        await h.Grain.EnsureRunningAsync();
        MakeHot(h.ShardOf(1));

        await CapturedSamplingTick(h.Timers)(CancellationToken.None);

        await h.SplitGrain.Received(1).SplitAsync(1);
    }
}
