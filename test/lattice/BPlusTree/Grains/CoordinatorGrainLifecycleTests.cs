using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Direct unit coverage for the reminder-anchored work-pump lifecycle on
/// <see cref="CoordinatorGrain{TSelf}"/> - the keepalive reminder, the phase
/// grain-timer, and the defensive arms that keep a transient reminder-storage
/// failure from faulting a coordinator on shutdown. Exercised on the base type
/// itself through a test-only derived coordinator so a regression cannot be
/// masked by a derived grain's own overrides.
/// <para>
/// The deactivation-hook bridge is covered separately by
/// <see cref="CoordinatorGrainDeactivationTests"/>.
/// </para>
/// </summary>
[TestFixture]
public class CoordinatorGrainLifecycleTests
{
    private const string GrainKey = "tree-a/0";

    /// <summary>
    /// Minimal derived coordinator that leaves every virtual member at its base
    /// default, so the base-class defaults (<c>LogContext</c>, the timer and
    /// reminder periods, the activation hook) are the behaviour under test.
    /// </summary>
    private sealed class TestCoordinator(
        IGrainContext context,
        IReminderRegistry reminderRegistry,
        ILogger<TestCoordinator> logger)
        : CoordinatorGrain<TestCoordinator>(context, reminderRegistry, logger)
    {
        public const string ReminderName = "test-coordinator-keepalive";

        public bool Outstanding { get; set; }
        public int PhaseCalls { get; private set; }
        public Exception? PhaseThrow { get; set; }

        protected override string KeepaliveReminderName => ReminderName;
        protected override bool InProgress => Outstanding;

        protected internal override Task ProcessNextPhaseAsync()
        {
            PhaseCalls++;
            if (PhaseThrow is not null) throw PhaseThrow;
            return Task.CompletedTask;
        }

        // Test surface onto the protected lifecycle members.
        public Task StartAsync() => StartCoordinatorAsync();
        public Task CompleteAsync() => CompleteCoordinatorAsync();
        public Task UnregisterAsync() => UnregisterKeepaliveAsync();
        public void ArmPhaseTimer() => StartPhaseTimer();
        public Task ActivateAsync(CancellationToken ct) => ((IGrainBase)this).OnActivateAsync(ct);

        public string ExposedLogContext => LogContext;
        public TimeSpan ExposedPhasePeriod => PhaseTimerPeriod;
        public TimeSpan ExposedKeepalivePeriod => KeepaliveReminderPeriod;
        public IReminderRegistry ExposedReminderRegistry => ReminderRegistry;
        public ILogger<TestCoordinator> ExposedLogger => Logger;
        public IGrainContext ExposedContext => Context;
    }

    private sealed record Harness(
        TestCoordinator Grain,
        IGrainContext Context,
        IReminderRegistry Reminders,
        ITimerRegistry Timers,
        IGrainTimer Timer);

    private static Harness Create(string key = GrainKey)
    {
        var timer = Substitute.For<IGrainTimer>();
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(timer);

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("test-coordinator", key));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var reminders = Substitute.For<IReminderRegistry>();
        var grain = new TestCoordinator(context, reminders, NullLogger<TestCoordinator>.Instance);
        return new Harness(grain, context, reminders, timerRegistry, timer);
    }

    /// <summary>
    /// The <c>Func&lt;CancellationToken, Task&gt;</c> the coordinator handed the
    /// timer registry - the phase tick. Firing it directly makes the tick
    /// deterministic with no sleeping.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTick(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private static GrainTimerCreationOptions CapturedTimerOptions(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (GrainTimerCreationOptions)call.GetArguments()[3]!;
    }

    /// <summary>
    /// How many phase timers the coordinator has registered. Counted off the
    /// recorded calls rather than through a typed NSubstitute matcher, because
    /// <see cref="ITimerRegistry.RegisterGrainTimer{TState}"/> is generic and its
    /// state argument is an internal closure type.
    /// </summary>
    private static int TimersRegistered(ITimerRegistry registry) =>
        registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    // ----------------------------------------------------------------- defaults

    [Test]
    public void LogContext_defaults_to_the_grain_key()
    {
        var h = Create();

        Assert.That(h.Grain.ExposedLogContext, Is.EqualTo(GrainKey));
    }

    [Test]
    public void Timer_and_reminder_periods_default_to_two_seconds_and_one_minute()
    {
        var h = Create();

        Assert.Multiple(() =>
        {
            Assert.That(h.Grain.ExposedPhasePeriod, Is.EqualTo(TimeSpan.FromSeconds(2)));
            Assert.That(h.Grain.ExposedKeepalivePeriod, Is.EqualTo(TimeSpan.FromMinutes(1)),
                "One minute is the Orleans reminder minimum.");
        });
    }

    [Test]
    public void Protected_accessors_expose_the_injected_dependencies()
    {
        var h = Create();

        Assert.Multiple(() =>
        {
            Assert.That(h.Grain.ExposedReminderRegistry, Is.SameAs(h.Reminders));
            Assert.That(h.Grain.ExposedContext, Is.SameAs(h.Context));
            Assert.That(h.Grain.ExposedLogger, Is.SameAs(NullLogger<TestCoordinator>.Instance));
            Assert.That(((IGrainBase)h.Grain).GrainContext, Is.SameAs(h.Context));
        });
    }

    [Test]
    public void Default_OnActivateCoreAsync_is_a_no_op_that_does_not_arm_the_phase_timer()
    {
        // A one-shot coordinator must not begin processing merely because
        // something activated it - only StartCoordinatorAsync starts the pump.
        var h = Create();

        Assert.That(async () => await h.Grain.ActivateAsync(CancellationToken.None), Throws.Nothing);
        Assert.That(TimersRegistered(h.Timers), Is.Zero);
    }

    // -------------------------------------------------------------------- start

    [Test]
    public async Task StartCoordinator_registers_the_keepalive_reminder_and_arms_the_phase_timer()
    {
        var h = Create();

        await h.Grain.StartAsync();

        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            h.Context.GrainId,
            TestCoordinator.ReminderName,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(1));

        var options = CapturedTimerOptions(h.Timers);
        Assert.Multiple(() =>
        {
            Assert.That(options.DueTime, Is.EqualTo(TimeSpan.Zero),
                "The pump must take its first step immediately rather than waiting a full period.");
            Assert.That(options.Period, Is.EqualTo(TimeSpan.FromSeconds(2)));
        });
    }

    [Test]
    public async Task Arming_the_phase_timer_twice_registers_only_one_timer()
    {
        var h = Create();

        await h.Grain.StartAsync();
        h.Grain.ArmPhaseTimer();
        h.Grain.ArmPhaseTimer();

        h.Timers.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    // --------------------------------------------------------------- phase tick

    [Test]
    public async Task A_phase_tick_advances_the_derived_phase_machine()
    {
        var h = Create();
        await h.Grain.StartAsync();

        await CapturedTick(h.Timers)(CancellationToken.None);

        Assert.That(h.Grain.PhaseCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task A_failing_phase_tick_is_swallowed_so_the_pump_keeps_running()
    {
        var h = Create();
        await h.Grain.StartAsync();
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        var tick = CapturedTick(h.Timers);

        Assert.That(async () => await tick(CancellationToken.None), Throws.Nothing,
            "A faulting phase must not escape the tick, or Orleans would stop the timer.");

        // The next tick still runs, proving the failure did not disarm the pump.
        h.Grain.PhaseThrow = null;
        await tick(CancellationToken.None);
        Assert.That(h.Grain.PhaseCalls, Is.EqualTo(2));
    }

    // -------------------------------------------------------- keepalive unregister

    [Test]
    public async Task UnregisterKeepalive_unregisters_the_reminder_when_one_exists()
    {
        var h = Create();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(h.Context.GrainId, TestCoordinator.ReminderName).Returns(reminder);

        await h.Grain.UnregisterAsync();

        await h.Reminders.Received(1).UnregisterReminder(h.Context.GrainId, reminder);
    }

    [Test]
    public async Task UnregisterKeepalive_is_a_no_op_when_no_reminder_is_registered()
    {
        var h = Create();
        h.Reminders.GetReminder(h.Context.GrainId, TestCoordinator.ReminderName)
            .Returns((IGrainReminder?)null);

        await h.Grain.UnregisterAsync();

        await h.Reminders.DidNotReceive().UnregisterReminder(
            Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public void UnregisterKeepalive_swallows_a_failing_reminder_lookup()
    {
        var h = Create();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Throws(new TimeoutException("reminder table unreachable"));

        Assert.That(async () => await h.Grain.UnregisterAsync(), Throws.Nothing,
            "A transient reminder-storage failure on shutdown must not fault the grain.");
    }

    [Test]
    public void UnregisterKeepalive_swallows_a_failing_unregister()
    {
        var h = Create();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Substitute.For<IGrainReminder>());
        h.Reminders.UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>())
            .Throws(new TimeoutException("reminder table unreachable"));

        Assert.That(async () => await h.Grain.UnregisterAsync(), Throws.Nothing);
    }

    // ----------------------------------------------------------------- complete

    [Test]
    public async Task CompleteCoordinator_disposes_the_timer_and_unregisters_the_reminder()
    {
        var h = Create();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(h.Context.GrainId, TestCoordinator.ReminderName).Returns(reminder);
        await h.Grain.StartAsync();

        await h.Grain.CompleteAsync();

        h.Timer.Received(1).Dispose();
        await h.Reminders.Received(1).UnregisterReminder(h.Context.GrainId, reminder);
    }

    [Test]
    public async Task CompleteCoordinator_clears_the_timer_so_a_later_arm_registers_a_fresh_one()
    {
        var h = Create();
        await h.Grain.StartAsync();

        await h.Grain.CompleteAsync();
        h.Grain.ArmPhaseTimer();

        h.Timers.Received(2).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    [Test]
    public void CompleteCoordinator_tolerates_never_having_armed_a_timer()
    {
        var h = Create();

        Assert.That(async () => await h.Grain.CompleteAsync(), Throws.Nothing);
    }

    // ----------------------------------------------------------------- reminder

    [Test]
    public async Task An_unrelated_reminder_name_is_ignored_entirely()
    {
        var h = Create();
        h.Grain.Outstanding = false;

        await h.Grain.ReceiveReminder("some-other-reminder", new TickStatus());

        await h.Reminders.DidNotReceive().GetReminder(Arg.Any<GrainId>(), Arg.Any<string>());
        Assert.That(TimersRegistered(h.Timers), Is.Zero);
    }

    [Test]
    public async Task The_keepalive_re_arms_the_phase_timer_when_work_is_still_outstanding()
    {
        // The silo-restart path: a reminder reactivates the grain, and the base
        // class - not the derived one - is responsible for restarting the pump.
        var h = Create();
        h.Grain.Outstanding = true;

        await h.Grain.ReceiveReminder(TestCoordinator.ReminderName, new TickStatus());

        h.Timers.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    [Test]
    public async Task The_keepalive_does_not_re_arm_a_phase_timer_that_is_already_running()
    {
        var h = Create();
        h.Grain.Outstanding = true;
        await h.Grain.StartAsync();

        await h.Grain.ReceiveReminder(TestCoordinator.ReminderName, new TickStatus());

        h.Timers.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    [Test]
    public async Task The_keepalive_self_destructs_once_no_work_remains()
    {
        var h = Create();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(h.Context.GrainId, TestCoordinator.ReminderName).Returns(reminder);
        h.Grain.Outstanding = false;

        await h.Grain.ReceiveReminder(TestCoordinator.ReminderName, new TickStatus());

        await h.Reminders.Received(1).UnregisterReminder(h.Context.GrainId, reminder);
        Assert.That(TimersRegistered(h.Timers), Is.Zero);
    }

    [Test]
    public void The_keepalive_self_destruct_survives_a_failing_reminder_registry()
    {
        var h = Create();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Throws(new TimeoutException("reminder table unreachable"));
        h.Grain.Outstanding = false;

        Assert.That(
            async () => await h.Grain.ReceiveReminder(TestCoordinator.ReminderName, new TickStatus()),
            Throws.Nothing,
            "Deactivation must still proceed when the reminder table cannot be reached.");
    }
}
