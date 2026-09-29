using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Keepalive-registration coverage (issue #3682): a keepalive reminder that
/// cannot be registered must never leave the orchestrator latched as running
/// with no sweep timer.
/// <para>
/// Orleans' reminder service initializes after the silo is active, and a
/// registration inside that window waits and then throws "Reminder Service is
/// still initializing". The orchestrator used to latch <c>_running</c> before
/// registering and start its timer only after, so that throw left it claiming
/// to run while nothing ever swept. Every later <c>EnsureRunningAsync</c>
/// returned at the latch, so the caller-side retry documented on
/// <c>LatticeGrain.OnActivateAsync</c> could never take effect, and a tree
/// armed only by activation - a read-only tree - stayed unhealed.
/// </para>
/// </summary>
public partial class ShardHealingOrchestratorGrainTests
{
    private static OrleansException StillInitializing() => new(
        "Reminder Service is still initializing and it is taking a long time. Please retry again later.",
        new TimeoutException());

    private static int SweepTimersArmed(ITimerRegistry timers) =>
        timers.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    private static Func<CancellationToken, Task> CapturedSweepTick(ITimerRegistry timers)
    {
        var call = timers.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private static Task<IGrainReminder> KeepaliveRegistration(Harness h) =>
        h.Reminders.RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

    [Test]
    public async Task EnsureRunning_arms_the_sweep_timer()
    {
        var h = CreateGrain();

        await h.Grain.EnsureRunningAsync();

        Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1));
    }

    [Test]
    public async Task EnsureRunning_arms_the_sweep_even_while_the_reminder_service_is_still_initializing()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).ThrowsAsync(StillInitializing());

        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.Nothing,
            "the sweep is running, so the caller's request to ensure it is running has been met");
        Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1),
            "a keepalive that could not yet be registered must not cost this activation its sweep");
        await Task.CompletedTask;
    }

    [Test]
    public async Task EnsureRunning_surfaces_a_non_transient_reminder_fault_but_keeps_the_sweep_armed()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).ThrowsAsync(new InvalidOperationException("reminder table down"));

        Assert.That(async () => await h.Grain.EnsureRunningAsync(), Throws.InvalidOperationException);
        Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1));
        await Task.CompletedTask;
    }

    [Test]
    public async Task EnsureRunning_does_not_arm_a_second_timer_after_a_failed_registration()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).ThrowsAsync(StillInitializing());

        await h.Grain.EnsureRunningAsync();
        await h.Grain.EnsureRunningAsync();

        Assert.That(SweepTimersArmed(h.Timers), Is.EqualTo(1));
    }

    [Test]
    public async Task A_sweep_tick_retries_a_keepalive_registration_that_failed_while_initializing()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw StillInitializing(),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();

        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        await h.Reminders.Received(2).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_sweep_tick_stops_retrying_once_the_keepalive_is_registered()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).Returns(
            _ => throw StillInitializing(),
            _ => Task.FromResult(Substitute.For<IGrainReminder>()));
        await h.Grain.EnsureRunningAsync();
        var tick = CapturedSweepTick(h.Timers);

        await tick(CancellationToken.None);
        await tick(CancellationToken.None);

        await h.Reminders.Received(2).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_sweep_tick_does_not_re_register_a_keepalive_that_already_succeeded()
    {
        var h = CreateGrain();
        await h.Grain.EnsureRunningAsync();

        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "shard-healing", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task A_sweep_tick_still_sweeps_while_the_keepalive_registration_keeps_failing()
    {
        var h = CreateGrain();
        KeepaliveRegistration(h).ThrowsAsync(StillInitializing());
        await h.Grain.EnsureRunningAsync();

        await CapturedSweepTick(h.Timers)(CancellationToken.None);

        Assert.That(h.State.State.LastObservedAtTicks, Is.Not.EqualTo(0L),
            "the sweep is the work; the keepalive only brings the activation back after it is collected");
    }
}
