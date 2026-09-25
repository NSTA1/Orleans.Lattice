using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3572 on the cross-tree coordinator: a state
/// write that lands but is reported as an ETag conflict must not wedge the
/// coordinator. It must translate the fault, deactivate, and fail fast, and a
/// fresh activation over the same durable row must reach the same verdict
/// without re-preparing, re-deciding differently, or finalizing twice.
/// </summary>
public partial class LatticeCrossTreeTxGrainTests
{
    /// <summary>Participants and storage shared by every activation of one coordinator.</summary>
    private sealed class CoordinatorEtagHarness
    {
        public CoordinatorEtagHarness(params string[] treeIds)
        {
            Factory = Substitute.For<IGrainFactory>();
            foreach (var treeId in treeIds)
            {
                var sub = Substitute.For<IAtomicWriteGrain>();
                sub.PrepareForCoordinatorAsync(
                        Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                        Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
                    .Returns(CrossTreePrepareVote.Prepared);
                sub.FinalizeAsync(Arg.Any<bool>()).Returns(Task.CompletedTask);
                Participants[treeId] = sub;
                Factory.GetGrain<IAtomicWriteGrain>($"{treeId}/{OperationId}").Returns(sub);
            }

            Reminders = Substitute.For<IReminderRegistry>();
            Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

            Options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            Options.CurrentValue.Returns(new LatticeOptions());
            Options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

            ClusterIds = Substitute.For<ILatticeOriginClusterIdResolver>();
            ClusterIds.Resolve(Arg.Any<string>()).Returns(string.Empty);
        }

        public DurableStateRow<CrossTreeTxState> Row { get; } = new();

        public IGrainFactory Factory { get; }

        public Dictionary<string, IAtomicWriteGrain> Participants { get; } = new(StringComparer.Ordinal);

        public IReminderRegistry Reminders { get; }

        public IOptionsMonitor<LatticeOptions> Options { get; }

        public ILatticeOriginClusterIdResolver ClusterIds { get; }

        public (LatticeCrossTreeTxGrain Grain, LandedConflictPersistentState<CrossTreeTxState> State, IGrainContext Context) Activate()
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("cross-tree-tx", OperationId));
            context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
            var state = new LandedConflictPersistentState<CrossTreeTxState>(Row);
            var grain = new LatticeCrossTreeTxGrain(
                context, Factory, Reminders, Options, ClusterIds,
                new LoggerFactory().CreateLogger<LatticeCrossTreeTxGrain>(), state);
            return (grain, state, context);
        }
    }

    private static void AssertTranslatedCoordinatorConflict(Exception? ex)
    {
        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>(),
            "the provider's conflict must be translated at the grain boundary, never leaked");
        var translated = (LatticeStateWriteFailedException)ex!;
        Assert.Multiple(() =>
        {
            Assert.That(translated.Conflict, Is.True);
            Assert.That(translated.GrainType, Is.EqualTo("cross-tree-tx"));
            Assert.That(translated.GrainKey, Is.EqualTo(OperationId));
            Assert.That(translated.InnerException, Is.Null);
        });
    }

    private const string CoordinatorRetentionReminder = "cross-tree-tx-retention";

    private static Task AssertCoordinatorRetentionArmedAsync(CoordinatorEtagHarness h, int times) =>
        h.Reminders.Received(times).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), CoordinatorRetentionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

    private static void VoteSequence(IAtomicWriteGrain participant, params CrossTreePrepareVote[] votes) =>
        participant.PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(votes[0], votes[1..]);

    [TestCase((int)CrossTreeTxPhase.Preparing)]
    [TestCase((int)CrossTreeTxPhase.Committed)]
    public async Task CommitAsync_landed_conflict_deactivates_then_fresh_activation_reaches_the_same_verdict(int conflictedPhase)
    {
        var conflictedWrite = (CrossTreeTxPhase)conflictedPhase;
        var h = new CoordinatorEtagHarness("orders", "inventory");
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == conflictedWrite;

        var ex = Assert.CatchAsync(() => grain.CommitAsync(batches));

        AssertTranslatedCoordinatorConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(conflictedWrite), "the conflicted write landed");

        var attemptsBefore = state.WriteAttempts;
        Assert.That(
            async () => await grain.CommitAsync(batches),
            Throws.TypeOf<LatticeStateWriteFailedException>().With.Property(nameof(LatticeStateWriteFailedException.Conflict)).True,
            "the conflicted activation fails fast");
        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore));

        var (fresh, freshState, _) = h.Activate();
        var outcome = await fresh.CommitAsync(batches);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
            Assert.That(freshState.StaleEtagRejections, Is.Zero);
        });
        foreach (var participant in h.Participants.Values)
        {
            await participant.Received(1).PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>());
            await participant.Received(1).FinalizeAsync(true);
            await participant.DidNotReceive().FinalizeAsync(false);
        }

        await AssertCoordinatorRetentionArmedAsync(h, 1);
    }

    [Test]
    public async Task CommitAsync_landed_conflict_on_completed_is_confirmed_and_terminal_cleanup_runs_once()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == CrossTreeTxPhase.Completed;

        var outcome = await grain.CommitAsync(batches);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
            Assert.That(state.Reads, Is.EqualTo(1), "the conflict is resolved by re-reading the row");
        });
        await AssertCoordinatorRetentionArmedAsync(h, 1);
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());

        var attemptsBefore = state.WriteAttempts;
        Assert.That(await grain.CommitAsync(batches), Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore));
        await AssertCoordinatorRetentionArmedAsync(h, 1);
        foreach (var participant in h.Participants.Values)
        {
            await participant.Received(1).FinalizeAsync(true);
        }
    }

    [Test]
    public async Task CommitAsync_landed_completed_conflict_with_a_failed_reread_is_cleaned_up_on_re_attach()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == CrossTreeTxPhase.Completed;
        state.FailNextReadWith = new TimeoutException("read blip");

        var ex = Assert.CatchAsync(() => grain.CommitAsync(batches));

        AssertTranslatedCoordinatorConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed), "the terminal write landed");
        await AssertCoordinatorRetentionArmedAsync(h, 0);

        var (fresh, freshState, _) = h.Activate();
        var outcome = await fresh.CommitAsync(batches);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(freshState.WriteAttempts, Is.Zero, "a completed coordinator is re-attached, not re-run");
        });
        await AssertCoordinatorRetentionArmedAsync(h, 1);
        foreach (var participant in h.Participants.Values)
        {
            await participant.Received(1).FinalizeAsync(true);
        }
    }

    [Test]
    public async Task ReceiveReminder_keepalive_on_a_completed_coordinator_arms_retention()
    {
        var h = new CoordinatorEtagHarness();
        h.Row.Value = new CrossTreeTxState
        {
            Phase = CrossTreeTxPhase.Completed,
            OperationId = OperationId,
            Outcome = CrossTreeAtomicWriteOutcome.Committed,
        };
        var (grain, _, context) = h.Activate();

        await grain.ReceiveReminder("cross-tree-tx-keepalive", new TickStatus());

        await AssertCoordinatorRetentionArmedAsync(h, 1);
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task CommitAsync_decision_conflict_that_did_not_land_never_serves_the_unpersisted_verdict()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        // A transient fault makes the first prepare vote Failed; the sub-saga
        // stays in Execute, so the re-dispatch votes Prepared.
        VoteSequence(h.Participants["inventory"], CrossTreePrepareVote.Failed, CrossTreePrepareVote.Prepared);
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, context) = h.Activate();
        state.FailWithoutLandingWhen = s => s.Phase == CrossTreeTxPhase.Aborted;

        var ex = Assert.CatchAsync(() => grain.CommitAsync(batches));

        AssertTranslatedCoordinatorConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing), "the abort decision did not land");
        Assert.That(
            async () => await grain.GetDecisionAsync(),
            Throws.TypeOf<LatticeStateWriteFailedException>().With.Property(nameof(LatticeStateWriteFailedException.Conflict)).True,
            "the in-memory Aborted verdict must never be served, or a registry would cache it");

        var (fresh, _, _) = h.Activate();
        var outcome = await fresh.CommitAsync(batches);
        var decision = await fresh.GetDecisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed),
                "the fresh activation re-decides from the durable Preparing phase");
            Assert.That(decision, Is.EqualTo(TxStatus.Committed));
        });
        foreach (var participant in h.Participants.Values)
        {
            await participant.Received(1).FinalizeAsync(true);
            await participant.DidNotReceive().FinalizeAsync(false);
        }
    }

    [Test]
    public async Task CommitAsync_transient_decision_write_fault_reports_in_flight_until_the_decision_is_persisted()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, context) = h.Activate();
        state.FailWithoutLandingWhen = s => s.Phase == CrossTreeTxPhase.Committed;
        state.FailWithoutLandingException = new TimeoutException("storage blip");

        var ex = Assert.CatchAsync(() => grain.CommitAsync(batches));

        Assert.That(ex, Is.TypeOf<TimeoutException>(),
            "a client-loadable, non-conflict fault propagates unchanged and keeps the activation");
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing));
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight),
            "a decision that is not yet durable is not served");
        foreach (var participant in h.Participants.Values)
        {
            await participant.DidNotReceive().FinalizeAsync(Arg.Any<bool>());
        }

        var outcome = await grain.CommitAsync(batches);
        var decision = await grain.GetDecisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
            Assert.That(decision, Is.EqualTo(TxStatus.Committed));
        });
        foreach (var participant in h.Participants.Values)
        {
            await participant.Received(1).PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>());
            await participant.Received(1).FinalizeAsync(true);
        }
    }

    [Test]
    public async Task CommitAsync_landed_conflict_on_an_abort_decision_is_not_flipped_by_a_fresh_activation()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        h.Participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(CrossTreePrepareVote.PreconditionFailed);
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == CrossTreeTxPhase.Aborted;

        Assert.CatchAsync(() => grain.CommitAsync(batches));

        var (fresh, _, _) = h.Activate();
        var outcome = await fresh.CommitAsync(batches);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.PreconditionFailed));
        await h.Participants["orders"].Received(1).FinalizeAsync(false);
        await h.Participants["orders"].DidNotReceive().FinalizeAsync(true);
    }

    [Test]
    public async Task CommitAsync_landed_conflict_on_a_vacuous_commit_is_confirmed_and_arms_retention()
    {
        var h = new CoordinatorEtagHarness();
        var (grain, state, _) = h.Activate();
        state.LandThenConflictOnNextWrite();

        var outcome = await grain.CommitAsync([]);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
            Assert.That(state.Reads, Is.EqualTo(1));
        });
        await AssertCoordinatorRetentionArmedAsync(h, 1);

        var (fresh, freshState, _) = h.Activate();
        Assert.That(await fresh.CommitAsync([]), Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(freshState.WriteAttempts, Is.Zero, "the landed vacuous commit is re-attached, not rewritten");
    }

    [Test]
    public async Task CommitAsync_landed_vacuous_conflict_with_a_failed_reread_arms_retention_on_re_attach()
    {
        var h = new CoordinatorEtagHarness();
        var (grain, state, _) = h.Activate();
        state.LandThenConflictOnNextWrite();
        state.FailNextReadWith = new TimeoutException("read blip");

        var ex = Assert.CatchAsync(() => grain.CommitAsync([]));

        AssertTranslatedCoordinatorConflict(ex);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Completed), "the vacuous commit landed");
        await AssertCoordinatorRetentionArmedAsync(h, 0);

        // A vacuous commit has no keepalive, so the re-attach is the only
        // trigger left to arm retention; without it the row would leak.
        var (fresh, freshState, _) = h.Activate();
        var outcome = await fresh.CommitAsync([]);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(freshState.WriteAttempts, Is.Zero);
        await AssertCoordinatorRetentionArmedAsync(h, 1);
    }

    [Test]
    public void CommitAsync_participant_conflict_propagates_and_leaves_the_coordinator_preparing()
    {
        var h = new CoordinatorEtagHarness("orders", "inventory");
        h.Participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .ThrowsAsync(new LatticeStateWriteFailedException(
                "atomic-write", $"inventory/{OperationId}", new InconsistentStateException("etag"), conflict: true));
        var (grain, _, context) = h.Activate();

        var ex = Assert.CatchAsync(() => grain.CommitAsync(Batches(
            ("orders", "order:1", "A"), ("inventory", "sku:1", "B"))));

        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>());
        Assert.That(((LatticeStateWriteFailedException)ex!).GrainType, Is.EqualTo("atomic-write"),
            "the participant's attribution must reach the caller unchanged");
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing),
            "a participant conflict is retryable, so the coordinator must not decide");
        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
    }

    [Test]
    public async Task ReceiveReminder_retention_clear_that_lands_but_reports_conflict_is_recovered()
    {
        var h = new CoordinatorEtagHarness();
        h.Row.Value = new CrossTreeTxState { Phase = CrossTreeTxPhase.Completed, OperationId = OperationId };
        var (grain, state, _) = h.Activate();
        state.LandThenConflictOnNextClear = true;

        await grain.ReceiveReminder("cross-tree-tx-retention", new TickStatus());

        Assert.That(h.Row.Exists, Is.False);
        Assert.That(state.Reads, Is.EqualTo(1));
    }
}
