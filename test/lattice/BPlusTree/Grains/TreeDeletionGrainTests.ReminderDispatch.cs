using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Branch coverage for <see cref="TreeDeletionGrain.ReceiveReminder"/> (issue #2347).
/// The <c>PurgeComplete</c> guard at the top of the method is the single teardown
/// path for both the purge reminder and the keepalive reminder: once a purge has
/// completed, either reminder must unregister both reminders and deactivate the
/// grain, and must never restart the purge. These tests pin that guard through the
/// real reminder entry point, so perturbing the production guard turns them red.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private const string PurgeReminderName = "tree-deletion";
    private const string KeepaliveReminderName = "deletion-keepalive";

    private sealed record ReminderHarness(
        TreeDeletionGrain Grain,
        FakePersistentState<TreeDeletionState> State,
        IReminderRegistry Reminders,
        IGrainContext Context,
        ITimerRegistry Timers,
        IGrainReminder PurgeReminder,
        IGrainReminder KeepaliveReminder);

    private static ReminderHarness CreateReminderHarness(Action<TreeDeletionState> arrange)
    {
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("deletion", TreeId));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var existingState = new FakePersistentState<TreeDeletionState>();
        arrange(existingState.State);

        var (grain, state, reminders, _, _) = CreateGrain(
            options: new LatticeOptions { SoftDeleteDuration = TimeSpan.FromHours(1) },
            existingState: existingState,
            grainContext: context);

        var purgeReminder = Substitute.For<IGrainReminder>();
        var keepaliveReminder = Substitute.For<IGrainReminder>();
        reminders.GetReminder(Arg.Any<GrainId>(), PurgeReminderName)
            .Returns(Task.FromResult<IGrainReminder?>(purgeReminder));
        reminders.GetReminder(Arg.Any<GrainId>(), KeepaliveReminderName)
            .Returns(Task.FromResult<IGrainReminder?>(keepaliveReminder));

        return new ReminderHarness(grain, state, reminders, context, timerRegistry, purgeReminder, keepaliveReminder);
    }

    private static int TimerRegistrations(ITimerRegistry timers) =>
        timers.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    private static int Deactivations(IGrainContext context) =>
        context.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IGrainContext.Deactivate));

    private static int Unregistrations(IReminderRegistry reminders, IGrainReminder reminder) =>
        reminders.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IReminderRegistry.UnregisterReminder)
            && ReferenceEquals(c.GetArguments()[1], reminder));

    private static void AssertTornDown(ReminderHarness h)
    {
        Assert.Multiple(() =>
        {
            Assert.That(Unregistrations(h.Reminders, h.PurgeReminder), Is.EqualTo(1), "the purge reminder should be unregistered");
            Assert.That(Unregistrations(h.Reminders, h.KeepaliveReminder), Is.EqualTo(1), "the keepalive reminder should be unregistered");
            Assert.That(Deactivations(h.Context), Is.EqualTo(1), "the grain should request deactivation");
            Assert.That(TimerRegistrations(h.Timers), Is.Zero, "a completed purge must never be restarted");
            Assert.That(h.State.State.PurgeComplete, Is.True, "teardown must not rewrite the completed state");
        });
        h.Context.Received(1).Deactivate(
            Arg.Is<DeactivationReason>(r => r.ReasonCode == DeactivationReasonCode.ApplicationRequested),
            Arg.Any<CancellationToken>());
    }

    private static void AssertUntouched(ReminderHarness h)
    {
        Assert.Multiple(() =>
        {
            Assert.That(Unregistrations(h.Reminders, h.PurgeReminder), Is.Zero);
            Assert.That(Unregistrations(h.Reminders, h.KeepaliveReminder), Is.Zero);
            Assert.That(Deactivations(h.Context), Is.Zero);
            Assert.That(TimerRegistrations(h.Timers), Is.Zero);
        });
    }

    // --- PurgeComplete teardown guard ---

    [Test]
    public async Task ReceiveReminder_purge_reminder_after_purge_complete_unregisters_reminders_and_deactivates()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.PurgeComplete = true;
        });

        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());

        AssertTornDown(h);
        Assert.That(h.State.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task ReceiveReminder_keepalive_after_purge_complete_unregisters_reminders_and_deactivates()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.PurgeComplete = true;
        });

        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        AssertTornDown(h);
    }

    [Test]
    public async Task ReceiveReminder_keepalive_after_purge_complete_tears_down_even_with_a_stale_in_progress_flag()
    {
        // The completed flag outranks a stale in-progress flag: the guard runs
        // before the keepalive resume arm, so a completed purge is never re-run.
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.PurgeComplete = true;
            s.PurgeInProgress = true;
            s.NextShardIndex = 1;
        });

        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        AssertTornDown(h);
        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the resume arm must not have rewritten the cursor");
    }

    // --- Remaining dispatch arms ---

    [Test]
    public async Task ReceiveReminder_on_a_live_tree_is_a_no_op()
    {
        var h = CreateReminderHarness(_ => { });

        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());
        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        AssertUntouched(h);
        Assert.That(h.State.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task ReceiveReminder_keepalive_with_no_purge_in_progress_is_a_no_op()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow;
        });

        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        AssertUntouched(h);
        Assert.That(h.State.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task ReceiveReminder_keepalive_with_purge_in_progress_resumes_from_the_persisted_shard()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.PurgeInProgress = true;
            s.NextShardIndex = 1;
        });

        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());
        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(TimerRegistrations(h.Timers), Is.EqualTo(1), "the purge timer should arm once and not re-arm while running");
            Assert.That(h.State.State.PurgeInProgress, Is.True);
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the purge should resume from the persisted shard");
            Assert.That(Deactivations(h.Context), Is.Zero);
        });
    }

    [Test]
    public async Task ReceiveReminder_purge_reminder_inside_the_soft_delete_window_is_a_no_op()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow;
        });

        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());

        AssertUntouched(h);
        Assert.That(h.State.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task ReceiveReminder_purge_reminder_after_the_soft_delete_window_starts_the_purge_from_shard_zero()
    {
        var h = CreateReminderHarness(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.NextShardIndex = 1;
        });

        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());
        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(TimerRegistrations(h.Timers), Is.EqualTo(1), "the purge timer should arm once and not re-arm while running");
            Assert.That(h.State.State.PurgeInProgress, Is.True);
            Assert.That(h.State.State.NextShardIndex, Is.Zero);
            Assert.That(Deactivations(h.Context), Is.Zero);
        });
    }
}
