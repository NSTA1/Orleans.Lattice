using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Reminder-service readiness coverage for <see cref="TreeDeletionGrain"/>
/// (issue #2579). Orleans' reminder service initialises asynchronously after the
/// silo reaches Active, so a registration issued inside that window can fail with
/// a transient "Reminder Service is still initializing" fault. The purge reminder
/// is the tree's only purge anchor and the idempotency guard at the top of
/// <c>DeleteTreeAsync</c> makes every later retry a silent no-op, so an unguarded
/// registration turns that transient into a permanently soft-deleted, unpurgeable
/// tree whose only symptom is silence.
/// </summary>
public partial class TreeDeletionGrainTests
{
    /// <summary>A backoff with the production attempt count but no real delay.</summary>
    private static readonly TimeSpan[] InstantBackoff =
    [
        TimeSpan.Zero,
        TimeSpan.Zero,
        TimeSpan.Zero,
        TimeSpan.Zero,
    ];

    private static Exception StillInitializing() =>
        new OrleansException(
            "Reminder Service is still initializing and it is taking a long time. "
            + "Please retry again later.",
            new TimeoutException());

    /// <summary>
    /// Counts registrations of <paramref name="reminderName"/> and fails the first
    /// <paramref name="transientAttempts"/> of them with <paramref name="fault"/>.
    /// Returns an accessor for the observed attempt count.
    /// </summary>
    private static Func<int> FailRegistrations(
        IReminderRegistry registry,
        string reminderName,
        int transientAttempts,
        Func<Exception> fault)
    {
        var attempts = 0;
        registry.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(),
                Arg.Is<string>(n => n == reminderName),
                Arg.Any<TimeSpan>(),
                Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                attempts++;
                if (attempts <= transientAttempts)
                {
                    throw fault();
                }

                return Task.FromResult(Substitute.For<IGrainReminder>());
            });

        return () => attempts;
    }

    // --- DeleteTreeAsync: the purge reminder ---

    [Test]
    public async Task DeleteTree_absorbs_a_transient_reminder_service_initializing_fault()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        grain.ReminderRegistrationBackoff = InstantBackoff;
        var attempts = FailRegistrations(
            reminderRegistry, "tree-deletion", transientAttempts: 2, StillInitializing);

        await grain.DeleteTreeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(attempts(), Is.EqualTo(3), "the registration should be retried past the transient");
            Assert.That(state.State.IsDeleted, Is.True, "the tree should be soft-deleted");
            Assert.That(state.State.DeletedAtUtc, Is.Not.Null);
        });
    }

    [Test]
    public async Task DeleteTree_leaves_no_tree_soft_deleted_without_a_purge_reminder()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        grain.ReminderRegistrationBackoff = InstantBackoff;
        var attempts = FailRegistrations(
            reminderRegistry, "tree-deletion", transientAttempts: int.MaxValue, StillInitializing);

        Assert.ThrowsAsync<OrleansException>(async () => await grain.DeleteTreeAsync());

        // The delete must not be left half-applied: a durable IsDeleted with no
        // purge reminder is unrecoverable through this grain, because the
        // idempotency guard makes the operator's retry a silent no-op.
        var stillDeleted = await grain.IsDeletedAsync();
        Assert.Multiple(() =>
        {
            Assert.That(attempts(), Is.EqualTo(InstantBackoff.Length + 1), "the whole retry budget should be spent");
            Assert.That(stillDeleted, Is.False, "the soft delete should have been rolled back");
            Assert.That(state.State.IsDeleted, Is.False, "the rollback should be persisted");
            Assert.That(state.State.DeletedAtUtc, Is.Null);
        });
    }

    [Test]
    public void DeleteTree_still_propagates_an_unrelated_reminder_fault()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        grain.ReminderRegistrationBackoff = InstantBackoff;
        var attempts = FailRegistrations(
            reminderRegistry,
            "tree-deletion",
            transientAttempts: int.MaxValue,
            () => new InvalidOperationException("reminder table is unreachable"));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DeleteTreeAsync());

        // The readiness guard is narrow by construction: an unrelated fault is not
        // retried at all, so it cannot mask a genuinely broken reminder service.
        Assert.That(attempts(), Is.EqualTo(1), "an unrelated fault should not consume a retry slot");
    }

    // --- BeginPurgeStateAsync: the keepalive reminder ---

    [Test]
    public async Task BeginPurge_absorbs_a_transient_reminder_service_initializing_fault()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        grain.ReminderRegistrationBackoff = InstantBackoff;
        var attempts = FailRegistrations(
            reminderRegistry, "deletion-keepalive", transientAttempts: 2, StillInitializing);

        await grain.BeginPurgeStateAsync(startFromShard: 0);

        Assert.Multiple(() =>
        {
            Assert.That(attempts(), Is.EqualTo(3), "the registration should be retried past the transient");
            Assert.That(state.State.PurgeInProgress, Is.True);
        });
    }
}
