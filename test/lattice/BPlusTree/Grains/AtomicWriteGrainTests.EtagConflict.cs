using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3572: an atomic-write saga state write that
/// <b>lands</b> but is reported as an ETag conflict (an Azure Table 412/409 after
/// a storage-SDK transport retry) must not wedge the saga. The activation must
/// translate the fault, deactivate, and fail fast; a fresh activation over the
/// same durable row must resume to the same verdict without double-applying the
/// batch or recording a second decision.
/// </summary>
public partial class AtomicWriteGrainTests
{
    /// <summary>
    /// Substitutes shared by every activation of one saga, so a test can model a
    /// fresh activation over the same durable row and the same downstream grains.
    /// </summary>
    private sealed class EtagConflictHarness
    {
        public EtagConflictHarness()
        {
            Factory = Substitute.For<IGrainFactory>();
            Lattice = Substitute.For<ILattice>();
            Factory.GetGrain<ILattice>(TreeId).Returns(Lattice);

            Shard = Substitute.For<IShardRootGrain>();
            Factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Shard);
            Shard.GetRawEntriesAsync(Arg.Any<List<string>>())
                .Returns(call => Task.FromResult(
                    new List<LwwEntry?>(new LwwEntry?[((List<string>)call[0]).Count])));
            Shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));

            Registry = Substitute.For<ITxRegistryGrain>();
            Registry.GetParticipantsAsync(Arg.Any<Guid>())
                .Returns(Task.FromResult<IReadOnlyList<int>>(new List<int>()));
            Factory.GetGrain<ITxRegistryGrain>(TreeId).Returns(Registry);

            var routing = new RoutingInfo(
                TreeId,
                ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount));
            Lattice.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(routing);
            Lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(routing);

            Reminders = Substitute.For<IReminderRegistry>();
            Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

            var options = new LatticeOptions { TxRegistryShardCount = 1 };
            Options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            Options.CurrentValue.Returns(options);
            Options.Get(Arg.Any<string>()).Returns(options);
        }

        public DurableStateRow<AtomicWriteState> Row { get; } = new();

        public IGrainFactory Factory { get; }

        public ILattice Lattice { get; }

        public IShardRootGrain Shard { get; }

        public ITxRegistryGrain Registry { get; }

        public IReminderRegistry Reminders { get; }

        public IOptionsMonitor<LatticeOptions> Options { get; }

        /// <summary>
        /// Activates the saga over <see cref="Row"/>, loading the durable state
        /// as the runtime does before activation.
        /// </summary>
        public (AtomicWriteGrain Grain, LandedConflictPersistentState<AtomicWriteState> State, IGrainContext Context) Activate()
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("atomic-write", $"{TreeId}/{OperationId}"));
            var state = new LandedConflictPersistentState<AtomicWriteState>(Row);
            var grain = new AtomicWriteGrain(
                context,
                Factory,
                Reminders,
                Options,
                new LoggerFactory().CreateLogger<AtomicWriteGrain>(),
                state);
            return (grain, state, context);
        }
    }

    private static void AssertTranslatedConflict(Exception? ex)
    {
        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>(),
            "the provider's conflict must be translated at the grain boundary, never leaked");
        var translated = (LatticeStateWriteFailedException)ex!;
        Assert.Multiple(() =>
        {
            Assert.That(translated.Conflict, Is.True);
            Assert.That(translated.GrainType, Is.EqualTo("atomic-write"));
            Assert.That(translated.GrainKey, Does.Contain(OperationId));
            Assert.That(translated.FaultType, Is.EqualTo(typeof(InconsistentStateException).FullName));
            Assert.That(translated.InnerException, Is.Null,
                "the provider exception is summarised, not carried, so a client can always load the fault");
        });
    }

    private const string RetentionReminder = "atomic-write-retention";

    private static Task AssertRetentionArmedAsync(EtagConflictHarness h, int times) =>
        h.Reminders.Received(times).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), RetentionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

    [TestCase("prepare")]
    [TestCase("execute-batch-commit")]
    public async Task ExecuteAsync_landed_conflict_deactivates_then_fresh_activation_completes_without_double_apply(string step)
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]), ("b", [2]));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = step == "prepare"
            ? s => s.Phase == AtomicWritePhase.Execute && s.NextIndex == 0
            : s => s.Phase == AtomicWritePhase.Execute && s.NextIndex == s.Entries.Count;

        var ex = Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));

        AssertTranslatedConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);

        // The conflicted activation fails fast without touching storage again.
        var attemptsBefore = state.WriteAttempts;
        var again = Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));
        Assert.That(again, Is.TypeOf<LatticeStateWriteFailedException>());
        Assert.That(((LatticeStateWriteFailedException)again!).Conflict, Is.True);
        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore),
            "a conflicted activation must not retry a write its stale ETag can never win");
        Assert.That(state.StaleEtagRejections, Is.Zero);

        // A fresh activation reloads what actually landed and resumes.
        var (fresh, freshState, _) = h.Activate();
        await fresh.ExecuteAsync(TreeId, entries);

        Assert.Multiple(() =>
        {
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(freshState.StaleEtagRejections, Is.Zero);
        });
        await h.Lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await h.Registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await h.Registry.DidNotReceive().MarkAbortedAsync(Arg.Any<Guid>());
        await AssertRetentionArmedAsync(h, 1);
    }

    [Test]
    public async Task ExecuteAsync_landed_conflict_on_complete_is_confirmed_and_terminal_cleanup_runs_once()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]), ("b", [2]));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Completed;

        await grain.ExecuteAsync(TreeId, entries);

        Assert.Multiple(() =>
        {
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(state.Reads, Is.EqualTo(1), "the conflict is resolved by re-reading the row");
            Assert.That(state.StaleEtagRejections, Is.Zero);
        });
        await AssertRetentionArmedAsync(h, 1);
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        await h.Registry.Received(1).ForgetAsync(Arg.Any<Guid>());
        await h.Lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await h.Registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());

        // The activation is trusted again: an idempotent re-entry succeeds and
        // neither rewrites the row nor re-arms retention.
        var attemptsBefore = state.WriteAttempts;
        await grain.ExecuteAsync(TreeId, entries);
        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore));
        await AssertRetentionArmedAsync(h, 1);
    }

    [Test]
    public async Task ExecuteAsync_landed_complete_conflict_with_a_failed_reread_is_cleaned_up_by_a_fresh_activation()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Completed;
        state.FailNextReadWith = new TimeoutException("read blip");

        var ex = Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));

        AssertTranslatedConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Completed), "the terminal write landed");
        await AssertRetentionArmedAsync(h, 0);
        await h.Registry.DidNotReceive().ForgetAsync(Arg.Any<Guid>());

        // The fresh activation short-circuits on the durable Completed phase and
        // must still arm retention and forget the decision, or the row leaks.
        var (fresh, freshState, _) = h.Activate();
        await fresh.ExecuteAsync(TreeId, entries);

        Assert.That(freshState.WriteAttempts, Is.Zero, "a completed saga is never re-applied");
        await AssertRetentionArmedAsync(h, 1);
        await h.Registry.Received(1).ForgetAsync(Arg.Any<Guid>());
        await h.Lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteAsync_complete_conflict_that_did_not_land_is_resumed_by_a_fresh_activation()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]));
        var (grain, state, context) = h.Activate();
        state.FailWithoutLandingWhen = s => s.Phase == AtomicWritePhase.Completed;

        var ex = Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));

        AssertTranslatedConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Execute), "the terminal write did not land");

        var (fresh, _, _) = h.Activate();
        await fresh.ExecuteAsync(TreeId, entries);

        Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await h.Lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        // The commit decision was recorded before the terminal write; the fresh
        // activation re-records the same decision, which the registry treats
        // idempotently. It never records the opposite one.
        await h.Registry.Received().MarkCommittedAsync(Arg.Any<Guid>());
        await h.Registry.DidNotReceive().MarkAbortedAsync(Arg.Any<Guid>());
        await AssertRetentionArmedAsync(h, 1);
    }

    [Test]
    public async Task ReceiveReminder_keepalive_on_a_completed_saga_arms_retention_and_forgets_the_decision()
    {
        var h = new EtagConflictHarness();
        var txid = Guid.NewGuid();
        h.Row.Value = new AtomicWriteState { Phase = AtomicWritePhase.Completed, TreeId = TreeId, TransactionId = txid };
        var (grain, _, context) = h.Activate();

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        await AssertRetentionArmedAsync(h, 1);
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        await h.Registry.Received(1).ForgetAsync(txid);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
    }

    [Test]
    public async Task ExecuteGuardedAsync_landed_conflict_on_precondition_failed_is_confirmed_and_arms_retention()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", ScoredJson(1)));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.PreconditionFailed;

        var outcome = await grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.PreconditionFailed));
            Assert.That(state.Reads, Is.EqualTo(1));
        });
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        await AssertRetentionArmedAsync(h, 1);
        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        await h.Lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteGuardedAsync_landed_precondition_failed_conflict_with_a_failed_reread_arms_retention_on_re_entry()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", ScoredJson(1)));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.PreconditionFailed;
        state.FailNextReadWith = new TimeoutException("read blip");

        var ex = Assert.CatchAsync(() => grain.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500)));

        AssertTranslatedConflict(ex);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.PreconditionFailed), "the terminal write landed");
        await AssertRetentionArmedAsync(h, 0);

        var (fresh, freshState, _) = h.Activate();
        var outcome = await fresh.ExecuteGuardedAsync(TreeId, entries, ScoreAtLeast(500));

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(AtomicWriteOutcome.PreconditionFailed));
            Assert.That(freshState.WriteAttempts, Is.Zero);
        });
        await AssertRetentionArmedAsync(h, 1);
        await h.Lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ExecuteAsync_landed_conflict_resumes_under_the_durable_transaction_id()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Execute && s.NextIndex == s.Entries.Count;

        Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));
        var durableTxid = h.Row.Value!.TransactionId;
        Assert.That(durableTxid, Is.Not.EqualTo(Guid.Empty));

        var (fresh, _, _) = h.Activate();
        await fresh.ExecuteAsync(TreeId, entries);

        await h.Registry.Received(1).MarkCommittedAsync(durableTxid);
    }

    [Test]
    public void ExecuteAsync_non_conflict_provider_fault_is_translated_without_deactivating()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.ThrowOnWrite = new ProviderOnlyStorageException("table unavailable");

        var ex = Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));

        Assert.That(ex, Is.TypeOf<LatticeStateWriteFailedException>());
        var translated = (LatticeStateWriteFailedException)ex!;
        Assert.Multiple(() =>
        {
            Assert.That(translated.Conflict, Is.False);
            Assert.That(translated.FaultType, Is.EqualTo(typeof(ProviderOnlyStorageException).FullName));
        });
    }

    [Test]
    public void ExecuteAsync_bcl_write_fault_propagates_unchanged()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.ThrowOnWrite = new TimeoutException("storage timeout");

        Assert.ThrowsAsync<TimeoutException>(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));
    }

    [Test]
    public async Task PrepareForCoordinatorAsync_landed_conflict_on_park_throws_then_fresh_activation_votes_prepared()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("k1", [1]));
        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Prepared;

        var ex = Assert.CatchAsync(() => grain.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1", participants: new[] { TreeId }));

        // Not a Failed vote: that would abort the cross-tree saga while this
        // sub-saga is durably parked, stranding it outside the finalize fan-out.
        AssertTranslatedConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Prepared), "the park write landed");

        var (fresh, _, _) = h.Activate();
        var vote = await fresh.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1", participants: new[] { TreeId });

        Assert.That(vote, Is.EqualTo(CrossTreePrepareVote.Prepared));
        await h.Registry.Received(1).RegisterExternalDecisionAuthorityAsync(Arg.Any<Guid>(), "xcoord-1");
        await h.Lattice.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task FinalizeAsync_landed_conflict_on_complete_is_confirmed_and_not_reapplied()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("k1", [1]));
        var (parked, _, _) = h.Activate();
        await parked.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1", participants: new[] { TreeId });

        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Completed;

        await grain.FinalizeAsync(commit: true);

        Assert.Multiple(() =>
        {
            Assert.That(h.Row.Value!.Phase, Is.EqualTo(AtomicWritePhase.Completed));
            Assert.That(state.Reads, Is.EqualTo(1));
        });
        await AssertRetentionArmedAsync(h, 1);

        var (fresh, freshState, _) = h.Activate();
        await fresh.FinalizeAsync(commit: true);

        Assert.That(freshState.WriteAttempts, Is.Zero);
        await h.Registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await h.Registry.DidNotReceive().MarkAbortedAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task FinalizeAsync_landed_complete_conflict_with_a_failed_reread_is_cleaned_up_by_a_fresh_activation()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("k1", [1]));
        var (parked, _, _) = h.Activate();
        await parked.PrepareForCoordinatorAsync(
            TreeId, entries, predicate: null, coordinatorKey: "xcoord-1", participants: new[] { TreeId });

        var (grain, state, context) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Completed;
        state.FailNextReadWith = new TimeoutException("read blip");

        var ex = Assert.CatchAsync(() => grain.FinalizeAsync(commit: true));

        AssertTranslatedConflict(ex);
        context.ReceivedWithAnyArgs(1).Deactivate(default!);
        Assert.That(
            async () => await grain.FinalizeAsync(commit: true),
            Throws.TypeOf<LatticeStateWriteFailedException>(),
            "the conflicted activation fails fast");
        await AssertRetentionArmedAsync(h, 0);

        var (fresh, freshState, _) = h.Activate();
        await fresh.FinalizeAsync(commit: true);

        Assert.That(freshState.WriteAttempts, Is.Zero);
        await AssertRetentionArmedAsync(h, 1);
        await h.Registry.Received(1).MarkCommittedAsync(Arg.Any<Guid>());
        await h.Registry.DidNotReceive().MarkAbortedAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task ReceiveReminder_keepalive_on_a_conflicted_activation_does_not_drive_the_saga()
    {
        var h = new EtagConflictHarness();
        var entries = MakeEntries(("a", [1]));
        var (grain, state, _) = h.Activate();
        state.LandThenConflictWhen = s => s.Phase == AtomicWritePhase.Execute && s.NextIndex == 0;
        Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, entries));
        var attemptsBefore = state.WriteAttempts;

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.WriteAttempts, Is.EqualTo(attemptsBefore));
        await h.Lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task ReceiveReminder_retention_clear_that_lands_but_reports_conflict_is_recovered()
    {
        var h = new EtagConflictHarness();
        h.Row.Value = new AtomicWriteState { Phase = AtomicWritePhase.Completed, TreeId = TreeId };
        var (grain, state, _) = h.Activate();
        state.LandThenConflictOnNextClear = true;

        await grain.ReceiveReminder("atomic-write-retention", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.Row.Exists, Is.False, "the landed clear must stand");
            Assert.That(state.Reads, Is.EqualTo(1), "the conflict must be resolved by re-reading the row");
        });
    }

    [Test]
    public async Task ReceiveReminder_retention_clear_conflict_keeps_a_row_that_is_no_longer_terminal()
    {
        var h = new EtagConflictHarness();
        h.Row.Value = new AtomicWriteState { Phase = AtomicWritePhase.Completed, TreeId = TreeId };
        var (grain, _, _) = h.Activate();

        // Another writer moved the row on behind this activation's back.
        h.Row.Value = new AtomicWriteState { Phase = AtomicWritePhase.Execute, TreeId = TreeId };
        h.Row.Etag++;

        await grain.ReceiveReminder("atomic-write-retention", new TickStatus());

        Assert.That(h.Row.Value?.Phase, Is.EqualTo(AtomicWritePhase.Execute),
            "a retention clear must never delete a saga that is no longer terminal");
    }

    /// <summary>
    /// Stands in for a storage provider's exception type, which lives in an
    /// assembly a client may not reference.
    /// </summary>
    private sealed class ProviderOnlyStorageException(string message) : Exception(message);
}
