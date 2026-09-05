using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the cross-tree coordinator's activation lifecycle: the
/// retention TTL reminder that clears an orphaned saga, the keepalive reminder's
/// per-phase dispatch (including its swallow-and-log arm), the completion
/// predicate, and the non-fatal reminder-registry failure arms.
/// </summary>
public partial class LatticeCrossTreeTxGrainTests
{
    private const string KeepaliveReminder = "cross-tree-tx-keepalive";
    private const string RetentionReminder = "cross-tree-tx-retention";

    // ---- Retention TTL ----------------------------------------------------

    [Test]
    public async Task Retention_reminder_clears_the_persisted_saga_state()
    {
        // The retention window is the last cleanup trigger for a terminal saga:
        // when it fires the persisted decision must be cleared, otherwise every
        // completed cross-tree write leaks a state row forever.
        var (grain, state, _, _) = CreateGrain(["orders"]);
        await grain.CommitAsync(Batches(("orders", "order:1", "A")));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));

        await grain.ReceiveReminder(RetentionReminder, default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted),
                "retention must clear the persisted state back to a fresh record");
            Assert.That(state.State.Participants, Is.Empty);
            Assert.That(state.State.Outcome, Is.Null);
        });
    }

    [Test]
    public async Task Retention_reminder_unregisters_itself_after_clearing()
    {
        var (grain, _, _, _) = CreateGrain(["orders"]);
        var reminders = ExtractReminderRegistry(grain);
        await grain.CommitAsync(Batches(("orders", "order:1", "A")));

        await grain.ReceiveReminder(RetentionReminder, default);

        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // ---- Keepalive dispatch ----------------------------------------------

    [Test]
    public async Task Keepalive_reminder_on_a_never_started_saga_unregisters_itself()
    {
        // An orphaned keepalive on a coordinator with no saga (state cleared by
        // retention, or a reminder that outlived its state) must retire itself
        // rather than tick forever against an empty activation.
        var (grain, state, _, _) = CreateGrain(["orders"]);
        var reminders = ExtractReminderRegistry(grain);
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted));

        await grain.ReceiveReminder(KeepaliveReminder, default);

        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted),
            "an orphaned keepalive must not start a saga");
    }

    [Test]
    public async Task Keepalive_reminder_ignores_an_unrelated_reminder_name()
    {
        // Only the keepalive name drives the phase machine; any other non-TTL
        // reminder that reaches this grain is a no-op.
        var state = new FakePersistentState<CrossTreeTxState>();
        state.State.Phase = CrossTreeTxPhase.Preparing;
        var (grain, _, _, participants) = CreateGrain(["orders"], state);

        await grain.ReceiveReminder("some-other-reminder", default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing));
        Assert.That(
            participants["orders"].ReceivedCalls().Any(c => c.GetMethodInfo().Name == "PrepareForCoordinatorAsync"),
            Is.False);
    }

    [Test]
    public async Task Keepalive_reminder_swallows_a_resume_failure_so_the_tick_can_retry()
    {
        // A reminder tick must never surface an exception to the reminder
        // service: the coordinator stays parked in Preparing and the NEXT tick
        // retries the whole prepare phase idempotently.
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);
        participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(Task.FromException<CrossTreePrepareVote>(new TimeoutException("still down")));

        Assert.ThrowsAsync<TimeoutException>(() => grain.CommitAsync(Batches(
            ("orders", "order:1", "A"), ("inventory", "sku:1", "B"))));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing));

        // The tick itself must not throw even though the resume still faults.
        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder(KeepaliveReminder, default));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing),
            "a failed resume leaves the coordinator parked for the next tick");
    }

    [Test]
    public async Task Keepalive_reminder_resumes_an_aborted_saga_through_finalize()
    {
        // The Aborted phase is resumable in exactly the same way as Committed:
        // a crash after the abort decision is durable but before the finalize
        // fan-out must be driven to Completed by the keepalive.
        var state = new FakePersistentState<CrossTreeTxState>();
        state.State.OperationId = OperationId;
        state.State.Phase = CrossTreeTxPhase.Aborted;
        state.State.Outcome = CrossTreeAtomicWriteOutcome.PreconditionFailed;
        state.State.Participants =
        [
            new CrossTreeParticipant
            {
                TreeId = "orders",
                Entries = [new KeyValuePair<string, byte[]>("order:1", [1])],
                Vote = CrossTreePrepareVote.Prepared,
            },
        ];
        var (grain, _, _, participants) = CreateGrain(["orders"], state);

        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        await participants["orders"].Received(1).FinalizeAsync(false);
    }

    // ---- Completion predicate --------------------------------------------

    [Test]
    public async Task IsCompleteAsync_is_true_only_for_a_terminal_or_unstarted_saga()
    {
        var (grain, state, _, _) = CreateGrain(["orders"]);

        Assert.That(await grain.IsCompleteAsync(), Is.True, "an unstarted coordinator holds nothing in flight");

        state.State.Phase = CrossTreeTxPhase.Preparing;
        Assert.That(await grain.IsCompleteAsync(), Is.False);

        state.State.Phase = CrossTreeTxPhase.Committed;
        Assert.That(await grain.IsCompleteAsync(), Is.False, "a decided-but-unfinalized saga is still in flight");

        state.State.Phase = CrossTreeTxPhase.Aborted;
        Assert.That(await grain.IsCompleteAsync(), Is.False);

        state.State.Phase = CrossTreeTxPhase.Completed;
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    // ---- Non-fatal reminder-registry failures -----------------------------

    [Test]
    public async Task CommitAsync_survives_a_keepalive_registration_failure()
    {
        // Keepalive registration is best-effort: it buys crash recovery, so a
        // reminder-service hiccup must degrade recovery rather than fail the
        // caller's write.
        var (grain, state, _, participants) = CreateGrain(["orders"]);
        var reminders = ExtractReminderRegistry(grain);
        reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), KeepaliveReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .ThrowsAsync(new InvalidOperationException("reminder service unavailable"));

        var outcome = await grain.CommitAsync(Batches(("orders", "order:1", "A")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        await participants["orders"].Received(1).FinalizeAsync(true);
    }

    [Test]
    public async Task CommitAsync_survives_a_keepalive_unregistration_failure()
    {
        var (grain, state, _, _) = CreateGrain(["orders"]);
        var reminders = ExtractReminderRegistry(grain);
        reminders.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unreachable"));

        var outcome = await grain.CommitAsync(Batches(("orders", "order:1", "A")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
    }

    [Test]
    public async Task CommitAsync_tolerates_an_absent_keepalive_reminder_on_unregister()
    {
        // GetReminder returning null is the ordinary "already gone" case and
        // must not attempt an unregister.
        var (grain, _, _, _) = CreateGrain(["orders"]);
        var reminders = ExtractReminderRegistry(grain);
        reminders.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder).Returns(Task.FromResult<IGrainReminder>(null!));

        await grain.CommitAsync(Batches(("orders", "order:1", "A")));

        await reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    // ---- Fingerprint encoding --------------------------------------------

    [Test]
    public async Task CommitAsync_fingerprints_a_key_too_long_for_the_stack_buffer()
    {
        // The fingerprint encodes each key through a 512-byte stack buffer and
        // rents from the array pool for anything longer. A key past that
        // threshold exercises the rented path, including its return to the pool.
        var longKey = new string('k', 600);
        var (grain, state, _, _) = CreateGrain(["orders"]);

        await grain.CommitAsync(Batches(("orders", longKey, "A")));

        Assert.That(state.State.Fingerprint, Is.Not.Null.And.Length.EqualTo(32));

        // And the fingerprint is still discriminating at that length: a
        // different long key on a resubmit must be rejected.
        state.State.Phase = CrossTreeTxPhase.Preparing;
        Assert.ThrowsAsync<LatticeIdempotencyKeyMismatchException>(() => grain.CommitAsync(
            Batches(("orders", new string('j', 600), "A"))));
    }

    [Test]
    public async Task CommitAsync_fingerprint_is_independent_of_leg_submission_order()
    {
        // Participants are sorted before fingerprinting, so the same logical
        // write submitted with its legs in the other order is an idempotent
        // replay, not a mismatch.
        var (grain, _, _, _) = CreateGrain(["inventory", "orders"]);
        await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));

        var replay = await grain.CommitAsync(Batches(("inventory", "sku:1", "B"), ("orders", "order:1", "A")));

        Assert.That(replay, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
    }
}
