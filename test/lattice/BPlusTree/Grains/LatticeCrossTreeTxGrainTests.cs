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
/// Unit coverage for <see cref="LatticeCrossTreeTxGrain"/>, the cross-tree
/// atomic-write coordinator. Drives the phase machine against substitute
/// participant sagas so commit / abort / failure / idempotency paths are
/// exercised without a silo.
/// </summary>
[TestFixture]
public class LatticeCrossTreeTxGrainTests
{
    private const string OperationId = "xop-1";

    private static (LatticeCrossTreeTxGrain grain,
                    FakePersistentState<CrossTreeTxState> state,
                    IGrainFactory grainFactory,
                    Dictionary<string, IAtomicWriteGrain> participants) CreateGrain(
        IEnumerable<string>? treeIds = null,
        FakePersistentState<CrossTreeTxState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cross-tree-tx", OperationId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var participants = new Dictionary<string, IAtomicWriteGrain>(StringComparer.Ordinal);
        foreach (var treeId in treeIds ?? [])
        {
            var sub = Substitute.For<IAtomicWriteGrain>();
            // Default happy-path stubs; individual tests override.
            sub.PrepareForCoordinatorAsync(
                    Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                    Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
                .Returns(CrossTreePrepareVote.Prepared);
            sub.FinalizeAsync(Arg.Any<bool>()).Returns(Task.CompletedTask);
            participants[treeId] = sub;
            grainFactory.GetGrain<IAtomicWriteGrain>($"{treeId}/{OperationId}").Returns(sub);
        }

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var state = existingState ?? new FakePersistentState<CrossTreeTxState>();
        var grain = new LatticeCrossTreeTxGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<LatticeCrossTreeTxGrain>(), state);
        return (grain, state, grainFactory, participants);
    }

    private static List<LatticeTreeBatch> Batches(params (string tree, string key, string value)[] rows)
    {
        var byTree = new Dictionary<string, List<KeyValuePair<string, byte[]>>>(StringComparer.Ordinal);
        var order = new List<string>();
        foreach (var (tree, key, value) in rows)
        {
            if (!byTree.TryGetValue(tree, out var list))
            {
                list = [];
                byTree[tree] = list;
                order.Add(tree);
            }
            list.Add(new KeyValuePair<string, byte[]>(key, System.Text.Encoding.UTF8.GetBytes(value)));
        }
        return order.Select(t => new LatticeTreeBatch(t, byTree[t])).ToList();
    }

    [Test]
    public async Task CommitAsync_all_prepared_commits_and_finalizes_with_commit()
    {
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);

        var outcome = await grain.CommitAsync(Batches(
            ("orders", "order:1", "A"),
            ("inventory", "sku:1", "B")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        await participants["orders"].Received(1).FinalizeAsync(true);
        await participants["inventory"].Received(1).FinalizeAsync(true);
    }

    [Test]
    public async Task CommitAsync_precondition_failed_aborts_and_finalizes_prepared_participants()
    {
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);
        participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(CrossTreePrepareVote.PreconditionFailed);

        var outcome = await grain.CommitAsync(Batches(
            ("orders", "order:1", "A"),
            ("inventory", "sku:1", "B")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.PreconditionFailed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        // Only the prepared participant has a staged sub-saga to abort.
        await participants["orders"].Received(1).FinalizeAsync(false);
        await participants["inventory"].DidNotReceive().FinalizeAsync(Arg.Any<bool>());
    }

    [Test]
    public void CommitAsync_genuine_failure_throws_after_aborting()
    {
        var (grain, _, _, participants) = CreateGrain(["orders", "inventory"]);
        participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(CrossTreePrepareVote.Failed);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.CommitAsync(Batches(
            ("orders", "order:1", "A"),
            ("inventory", "sku:1", "B"))));
    }

    [Test]
    public async Task CommitAsync_empty_batches_is_vacuous_commit()
    {
        var (grain, state, _, _) = CreateGrain();

        var outcome = await grain.CommitAsync([]);

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
    }

    [Test]
    public async Task CommitAsync_empty_batches_arms_retention_reminder()
    {
        var (grain, _, _, _) = CreateGrain();
        var reminderRegistry = ExtractReminderRegistry(grain);

        await grain.CommitAsync([]);

        // The vacuous-commit path never registers a keepalive, so the retention
        // TTL reminder is the only cleanup trigger and MUST be armed - otherwise
        // the persisted Completed state is orphaned forever.
        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "cross-tree-tx-retention",
            Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task Keepalive_reminder_on_completed_phase_arms_retention()
    {
        // Simulate a crash in FinalizePhaseAsync after the Completed phase was
        // persisted but before retention was armed: the keepalive reminder is
        // still registered and fires into the Completed branch, which must arm
        // retention so the orphaned state is eventually cleared.
        var state = new FakePersistentState<CrossTreeTxState>();
        state.State.Phase = CrossTreeTxPhase.Completed;
        state.State.Outcome = CrossTreeAtomicWriteOutcome.Committed;
        var (grain, _, _, _) = CreateGrain(["orders"], state);
        var reminderRegistry = ExtractReminderRegistry(grain);

        await grain.ReceiveReminder("cross-tree-tx-keepalive", default);

        await reminderRegistry.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "cross-tree-tx-retention",
            Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    private static IReminderRegistry ExtractReminderRegistry(LatticeCrossTreeTxGrain grain)
    {
        // The reminder registry is a captured primary-constructor parameter on
        // the base TtlGrain; its backing field name is compiler-generated and it
        // lives on a base type, so walk the hierarchy and match by type.
        for (var t = (Type?)typeof(LatticeCrossTreeTxGrain); t is not null; t = t.BaseType)
        {
            var field = t.GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                .FirstOrDefault(f => typeof(IReminderRegistry).IsAssignableFrom(f.FieldType));
            if (field is not null)
            {
                return (IReminderRegistry)field.GetValue(grain)!;
            }
        }
        throw new InvalidOperationException("No IReminderRegistry field found on the grain hierarchy.");
    }

    [Test]
    public void CommitAsync_duplicate_tree_id_throws()
    {
        var (grain, _, _, _) = CreateGrain(["orders"]);
        var batches = new List<LatticeTreeBatch>
        {
            new("orders", [new("a", [1])]),
            new("orders", [new("b", [2])]),
        };

        Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync(batches));
    }

    [Test]
    public void CommitAsync_null_batches_throws()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.CommitAsync(null!));
    }

    [Test]
    public async Task CommitAsync_resubmit_with_same_keyset_returns_memoized_outcome()
    {
        var (grain, _, _, participants) = CreateGrain(["orders", "inventory"]);
        var batches = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));

        await grain.CommitAsync(batches);
        var second = await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));

        Assert.That(second, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        // No re-prepare on the memoized re-attach.
        await participants["orders"].Received(1).PrepareForCoordinatorAsync(
            Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>());
    }

    [Test]
    public async Task CommitAsync_resubmit_with_different_keyset_throws()
    {
        var state = new FakePersistentState<CrossTreeTxState>();
        var (grain, _, _, _) = CreateGrain(["orders", "inventory"], state);
        await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));

        // Force the in-flight stability branch, then re-submit a changed key set.
        // The mismatch is a caller error, surfaced as the dedicated typed
        // exception (which derives from InvalidOperationException) with a
        // self-contained message that does not mention cluster logs.
        state.State.Phase = CrossTreeTxPhase.Preparing;
        var ex = Assert.ThrowsAsync<LatticeIdempotencyKeyMismatchException>(() => grain.CommitAsync(
            Batches(("orders", "order:CHANGED", "A"), ("inventory", "sku:1", "B"))));
        Assert.That(ex!.Message, Does.Contain("different set of").And.Not.Contain("cluster logs"));
    }

    [Test]
    public async Task CommitAsync_resubmit_with_different_keyset_after_completion_throws()
    {
        // Issue #1402 item 2: the fingerprint-stability guard must also fire once
        // the coordinator has COMPLETED - not only while it is in flight. Before the
        // fix a reused operationId with a changed tree/key set was silently replayed
        // as the original memoized verdict instead of failing the precondition, so
        // the single-tree and cross-tree idempotency contracts disagreed.
        var (grain, state, _, _) = CreateGrain(["orders", "inventory"]);
        var outcome = await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed), "the first submit runs to completion");
        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        var ex = Assert.ThrowsAsync<LatticeIdempotencyKeyMismatchException>(() => grain.CommitAsync(
            Batches(("orders", "order:CHANGED", "A"), ("inventory", "sku:1", "B"))));
        Assert.That(ex!.Message, Does.Contain("different set of").And.Not.Contain("cluster logs"));
    }

    [Test]
    public async Task CommitAsync_resubmit_with_same_keyset_after_completion_replays_memoized_outcome()
    {
        // The counterpart guarantee: an identical resubmit after completion stays a
        // safe idempotent replay (the fingerprint matches), returning the memoized
        // verdict rather than re-running the saga.
        var (grain, _, _, participants) = CreateGrain(["orders", "inventory"]);
        var rows = Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B"));
        await grain.CommitAsync(rows);

        var replay = await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));

        Assert.That(replay, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        // Finalize ran exactly once - the replay did not re-drive the coordinator.
        await participants["orders"].Received(1).FinalizeAsync(true);
    }

    [Test]
    public async Task GetDecisionAsync_reflects_phase()
    {
        var (grain, state, _, _) = CreateGrain(["orders"]);

        state.State.Phase = CrossTreeTxPhase.Preparing;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight));

        state.State.Phase = CrossTreeTxPhase.Committed;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));

        state.State.Phase = CrossTreeTxPhase.Aborted;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Aborted));

        state.State.Phase = CrossTreeTxPhase.Completed;
        state.State.Outcome = CrossTreeAtomicWriteOutcome.Committed;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));

        state.State.Outcome = CrossTreeAtomicWriteOutcome.PreconditionFailed;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task CommitAsync_defensively_copies_caller_entries()
    {
        var (grain, state, _, _) = CreateGrain(["orders"]);
        var value = new byte[] { 1, 2, 3 };
        var entries = new List<KeyValuePair<string, byte[]>> { new("order:1", value) };
        var batches = new List<LatticeTreeBatch> { new("orders", entries) };

        await grain.CommitAsync(batches);

        // Mutate the caller's buffer and list after submit.
        value[0] = 99;
        entries.Add(new KeyValuePair<string, byte[]>("order:2", [9]));

        var persisted = state.State.Participants.Single();
        Assert.That(persisted.Entries, Has.Count.EqualTo(1), "caller list mutation must not leak into persisted state");
        Assert.That(persisted.Entries[0].Value[0], Is.EqualTo(1), "caller buffer mutation must not leak into persisted state");
    }

    [Test]
    public async Task CommitAsync_passes_coordinator_key_to_participants()
    {
        var (grain, _, _, participants) = CreateGrain(["orders"]);

        await grain.CommitAsync(Batches(("orders", "order:1", "A")));

        await participants["orders"].Received(1).PrepareForCoordinatorAsync(
            "orders", Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode?>(), OperationId, Arg.Any<IReadOnlyList<string>>());
    }

    [Test]
    public async Task Keepalive_reminder_resumes_finalize_after_crash_between_decision_and_finalize()
    {
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);
        // Simulate a crash AFTER the single global decision is durable but DURING
        // the finalize fan-out: the first finalize round faults, parking the
        // coordinator at Committed with the decision already persisted.
        participants["orders"].FinalizeAsync(true)
            .Returns(Task.FromException(new TimeoutException("crash")));

        Assert.ThrowsAsync<TimeoutException>(() => grain.CommitAsync(Batches(
            ("orders", "order:1", "A"), ("inventory", "sku:1", "B"))));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Committed),
            "a crash mid-finalize must leave the durable Committed decision in place");

        // Recovery: clear the fault and let the keepalive reminder drive resume.
        participants["orders"].FinalizeAsync(true).Returns(Task.CompletedTask);
        await grain.ReceiveReminder("cross-tree-tx-keepalive", default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        await participants["orders"].Received(2).FinalizeAsync(true);
        await participants["inventory"].Received(2).FinalizeAsync(true);
    }

    [Test]
    public async Task Keepalive_reminder_resumes_prepare_after_crash_during_prepare()
    {
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);
        // Crash during prepare: one participant faults instead of voting, leaving
        // the coordinator parked at Preparing with no decision yet written.
        participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(Task.FromException<CrossTreePrepareVote>(new TimeoutException("crash")));

        Assert.ThrowsAsync<TimeoutException>(() => grain.CommitAsync(Batches(
            ("orders", "order:1", "A"), ("inventory", "sku:1", "B"))));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Preparing),
            "a crash before the decision must leave the coordinator pre-decision");

        // Recovery: the faulted participant now votes Prepared on retry.
        participants["inventory"].PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(CrossTreePrepareVote.Prepared);
        await grain.ReceiveReminder("cross-tree-tx-keepalive", default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        await participants["orders"].Received(1).FinalizeAsync(true);
        await participants["inventory"].Received(1).FinalizeAsync(true);
    }

    [Test]
    public async Task Keepalive_reminder_on_completed_saga_is_a_noop()
    {
        var (grain, state, _, participants) = CreateGrain(["orders"]);
        await grain.CommitAsync(Batches(("orders", "order:1", "A")));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));

        await grain.ReceiveReminder("cross-tree-tx-keepalive", default);

        // No additional finalize: a terminal saga must not re-run on a late tick.
        await participants["orders"].Received(1).FinalizeAsync(true);
    }
}
