using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Orleans' reminder service initialises asynchronously after the silo reaches
/// Active, so a tree deleted inside that window sees the transient "Reminder
/// Service is still initializing" fault when arming its purge reminder. That
/// registration happens after the deletion has already been persisted, so the
/// naive failure is not merely transient: the idempotency guard at the top of
/// <see cref="TreeDeletionGrain.DeleteTreeAsync"/> turns every retry into a
/// silent no-op, leaving the tree soft-deleted forever with nothing left to fire
/// its purge. These tests pin both halves of the fix - waiting the window out,
/// and leaving the delete retryable when it cannot be waited out.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private const string PurgeReminderName = "tree-deletion";
    private const string KeepaliveReminderName = "deletion-keepalive";

    private static Exception StillInitializing() =>
        new InvalidOperationException(
            "Reminder Service is still initializing and it is taking a long time. Please retry again later.",
            new TimeoutException("The operation has timed out."));

    /// <summary>
    /// A registry that throws the startup transient for its first
    /// <paramref name="failures"/> registrations and succeeds thereafter.
    /// </summary>
    private static IReminderRegistry RegistryFailingFirst(int failures)
    {
        var registry = Substitute.For<IReminderRegistry>();
        var seen = 0;
        registry
            .RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => ++seen <= failures
                ? Task.FromException<IGrainReminder>(StillInitializing())
                : Task.FromResult(Substitute.For<IGrainReminder>()));
        return registry;
    }

    private static IReminderRegistry RegistryAlwaysThrowing(Exception fault)
    {
        var registry = Substitute.For<IReminderRegistry>();
        registry
            .RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Throws(_ => fault);
        return registry;
    }

    [Test]
    public void ReminderServiceReadiness_recognises_the_fault_these_tests_inject()
    {
        // Guards the premise: if this ever goes false, every test below is
        // exercising the unrelated-fault path instead and proves nothing.
        Assert.That(ReminderServiceReadiness.IsStillInitializing(StillInitializing()), Is.True);
    }

    [Test]
    public void DeleteTree_waits_out_the_reminder_service_startup_window()
    {
        var registry = RegistryFailingFirst(failures: 2);
        var (grain, _, _, _, _) = CreateGrain(
            reminderRegistry: registry,
            registrationBackoff: [TimeSpan.Zero, TimeSpan.Zero]);

        Assert.That(
            async () => await grain.DeleteTreeAsync(),
            Throws.Nothing,
            "a delete issued inside the reminder-service startup window must wait the window out, "
            + "not fail after it has already persisted the deletion");
    }

    [Test]
    public async Task DeleteTree_arms_the_purge_reminder_once_the_reminder_service_is_ready()
    {
        var registry = RegistryFailingFirst(failures: 1);
        var (grain, _, _, _, _) = CreateGrain(
            reminderRegistry: registry,
            registrationBackoff: [TimeSpan.Zero]);

        await grain.DeleteTreeAsync();

        await registry.Received(2).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), PurgeReminderName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public void DeleteTree_leaves_no_tree_deleted_without_a_purge_reminder()
    {
        // The consequence that makes this more than a transient. The deletion is
        // persisted before the registration, so unless it is rolled back the retry
        // short-circuits on `if (state.State.IsDeleted) return;` and the purge
        // reminder is never armed by anyone.
        var (grain, state, _, _, _) = CreateGrain(
            reminderRegistry: RegistryAlwaysThrowing(StillInitializing()));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DeleteTreeAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.IsDeleted, Is.False,
                "a delete that could not arm its purge reminder must leave no durable trace, "
                + "or no retry can ever arm it");
            Assert.That(state.State.DeletedAtUtc, Is.Null);
            Assert.That(state.WriteCount, Is.GreaterThanOrEqualTo(2),
                "the rollback must be persisted, not merely applied in memory - the deletion "
                + "was already written to storage");
        });
    }

    [Test]
    public async Task DeleteTree_completes_on_a_retry_after_a_failed_registration()
    {
        // End to end over two calls: the first delete fails to arm the reminder,
        // the second must do real work rather than return early.
        var registry = Substitute.For<IReminderRegistry>();
        var fail = true;
        registry
            .RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => fail
                ? Task.FromException<IGrainReminder>(StillInitializing())
                : Task.FromResult(Substitute.For<IGrainReminder>()));

        var (grain, state, _, _, _) = CreateGrain(reminderRegistry: registry);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DeleteTreeAsync());

        fail = false;
        await grain.DeleteTreeAsync();

        Assert.That(state.State.IsDeleted, Is.True);
        await registry.Received(2).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), PurgeReminderName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task DeleteTree_still_propagates_an_unrelated_reminder_fault()
    {
        // The guard must stay narrow: only the "still initializing" transient is
        // waited out, and an unrelated fault must not consume a retry slot.
        var registry = RegistryAlwaysThrowing(new InvalidOperationException("reminder storage unavailable"));
        var (grain, _, _, _, _) = CreateGrain(
            reminderRegistry: registry,
            registrationBackoff: [TimeSpan.Zero, TimeSpan.Zero]);

        Assert.That(
            async () => await grain.DeleteTreeAsync(),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.EqualTo("reminder storage unavailable"));

        await registry.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), PurgeReminderName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task BeginPurgeState_waits_out_the_reminder_service_startup_window()
    {
        // No rollback is asserted here: unlike the purge reminder, the keepalive
        // has a natural re-attempt seam, because the already-armed tree-deletion
        // reminder re-enters StartPurgeAsync on its next tick.
        var registry = RegistryFailingFirst(failures: 1);
        var (grain, _, _, _, _) = CreateGrain(
            reminderRegistry: registry,
            registrationBackoff: [TimeSpan.Zero]);

        Assert.That(
            async () => await grain.BeginPurgeStateAsync(startFromShard: 0),
            Throws.Nothing);

        await registry.Received(2).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), KeepaliveReminderName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }
}
