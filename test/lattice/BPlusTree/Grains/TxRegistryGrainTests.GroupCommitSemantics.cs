using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Read-committed, failure and idempotency semantics of the registry's group
/// commit (issue #3475). Every test holds the first state write open on a gate,
/// so the calls that arrive meanwhile are observed against an un-durable
/// mutation, and runs the grain on one exclusive scheduler (see
/// <see cref="OnTurnAsync{T}"/>).
/// </summary>
public partial class TxRegistryGrainTests
{
    /// <summary>
    /// A state whose first write is held open until the returned gate is
    /// released, and optionally fails when it is.
    /// </summary>
    private static (FakePersistentState<TxRegistryState> state, TaskCompletionSource gate) GatedState(Exception? failFirstWrite = null)
    {
        var state = new FakePersistentState<TxRegistryState>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = true;
        state.BeforeWrite = () =>
        {
            if (!first) return Task.CompletedTask;
            first = false;
            if (failFirstWrite is not null) state.ThrowOnWrite = failFirstWrite;
            return gate.Task;
        };
        return (state, gate);
    }

    /// <summary>Waits for every continuation already queued on <paramref name="turn"/> to run.</summary>
    private static async Task DrainAsync(TaskScheduler turn)
    {
        for (var i = 0; i < 4; i++)
        {
            await OnTurnAsync(turn, () => Task.CompletedTask);
        }
    }

    [Test]
    public async Task GetStatusAsync_during_an_outstanding_commit_write_waits_and_returns_the_durable_verdict()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var read = OnTurnAsync(turn, () => grain.GetStatusAsync(txid));
        var many = OnTurnAsync(turn, () => grain.GetStatusManyAsync([txid]));
        var recorded = OnTurnAsync(turn, () => grain.GetRecordedStatusAsync(txid));
        await DrainAsync(turn);

        Assert.Multiple(() =>
        {
            Assert.That(read.IsCompleted, Is.False, "A reader must not return a verdict whose write is still outstanding.");
            Assert.That(many.IsCompleted, Is.False);
            Assert.That(recorded.IsCompleted, Is.False);
            Assert.That(mark.IsCompleted, Is.False, "MarkCommittedAsync must not acknowledge before the write is durable.");
        });

        gate.SetResult();
        await mark;
        var status = await read;
        var statuses = await many;
        var recordedStatus = await recorded;
        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(TxStatus.Committed));
            Assert.That(statuses[txid], Is.EqualTo(TxStatus.Committed));
            Assert.That(recordedStatus, Is.EqualTo(TxStatus.Committed));
        });
    }

    [Test]
    public async Task GetStatusAsync_for_an_untouched_txid_does_not_wait_behind_an_outstanding_write()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);

        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(Guid.NewGuid()));
        var other = Guid.NewGuid();
        var status = await OnTurnAsync(turn, () => grain.GetStatusAsync(other));
        var participants = await OnTurnAsync(turn, () => grain.GetParticipantsAsync(other));

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(TxStatus.InFlight));
            Assert.That(participants, Is.Empty);
            Assert.That(mark.IsCompleted, Is.False, "The write for the other saga must still be outstanding.");
        });
        gate.SetResult();
        await mark;
    }

    [Test]
    public async Task GetStatusAsync_when_the_outstanding_write_fails_recomputes_against_the_rolled_back_state()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var failure = new InvalidOperationException("storage down");
        var (state, gate) = GatedState(failure);
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var read = OnTurnAsync(turn, () => grain.GetStatusAsync(txid));
        await DrainAsync(turn);
        gate.SetResult();

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await mark);
        var status = await read;
        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure), "The storage exception must reach the caller unwrapped.");
            Assert.That(status, Is.EqualTo(TxStatus.InFlight),
                "A reader must never report a commit whose write failed.");
            Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
            Assert.That(state.State.DecisionsRevision, Is.Zero);
        });
    }

    [Test]
    public async Task MarkCommittedAsync_when_a_group_write_fails_faults_every_waiter_and_restores_the_durable_state()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var failure = new InvalidOperationException("storage down");
        var state = new FakePersistentState<TxRegistryState>();
        var (grain, _) = CreateGrain(state);
        var durable = Guid.NewGuid();
        // Seed a durable decision so the rollback must restore a non-empty state.
        await grain.MarkCommittedAsync(durable);
        var baseline = state.State.DecisionsRevision;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstGated = true;
        state.BeforeWrite = () =>
        {
            if (!firstGated) return Task.CompletedTask;
            firstGated = false;
            state.ThrowOnWrite = failure;
            return gate.Task;
        };

        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var c = Guid.NewGuid();
        // a is carried by the write that fails; b and c are queued behind it.
        var inFlight = OnTurnAsync(turn, () => grain.MarkCommittedAsync(a));
        var queuedCommit = OnTurnAsync(turn, () => grain.MarkCommittedAsync(b));
        var queuedAbort = OnTurnAsync(turn, () => grain.MarkAbortedAsync(c));
        var queuedParticipants = OnTurnAsync(turn, () => grain.RegisterParticipantsAsync(c, [1, 2]));
        await DrainAsync(turn);
        gate.SetResult();

        Assert.Multiple(() =>
        {
            Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await inFlight), Is.SameAs(failure));
            Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await queuedCommit), Is.SameAs(failure),
                "A mutation queued behind a failed write was applied on non-durable state and must fail too.");
            Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await queuedAbort), Is.SameAs(failure));
            Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await queuedParticipants), Is.SameAs(failure));
            Assert.That(state.State.Decisions.Keys, Is.EquivalentTo(new[] { durable }));
            Assert.That(state.State.Participants, Is.Empty);
            Assert.That(state.State.DecisionsRevision, Is.EqualTo(baseline));
        });

        // The next call starts from the durable state and succeeds.
        await OnTurnAsync(turn, () => grain.MarkCommittedAsync(a));
        Assert.That(state.State.Decisions[a], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkCommittedAsync_repeat_during_an_outstanding_write_waits_for_that_write()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var first = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var repeat = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        await DrainAsync(turn);

        Assert.That(repeat.IsCompleted, Is.False,
            "An idempotent repeat must not acknowledge a decision that is not yet durable.");
        gate.SetResult();
        await Task.WhenAll(first, repeat);
        Assert.That(state.WriteCount, Is.EqualTo(1), "The idempotent repeat must not issue a write of its own.");
    }

    [Test]
    public async Task MarkCommittedAsync_repeat_when_the_first_write_fails_records_the_decision_itself()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var failure = new InvalidOperationException("storage down");
        var (state, gate) = GatedState(failure);
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var first = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var repeat = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        await DrainAsync(turn);
        gate.SetResult();

        Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await first), Is.SameAs(failure));
        await repeat;
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkAbortedAsync_conflicting_with_a_commit_whose_write_fails_records_the_abort()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var failure = new InvalidOperationException("storage down");
        var (state, gate) = GatedState(failure);
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var commit = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var abort = OnTurnAsync(turn, () => grain.MarkAbortedAsync(txid));
        await DrainAsync(turn);
        gate.SetResult();

        Assert.That(Assert.ThrowsAsync<InvalidOperationException>(async () => await commit), Is.SameAs(failure));
        await abort;
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted),
            "A conflict against a verdict that never became durable is not a real conflict.");
    }

    [Test]
    public async Task MarkAbortedAsync_conflicting_with_a_durable_commit_still_throws()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var commit = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var abort = OnTurnAsync(turn, () => grain.MarkAbortedAsync(txid));
        await DrainAsync(turn);
        gate.SetResult();

        await commit;
        Assert.ThrowsAsync<InvalidOperationException>(async () => await abort);
    }

    [Test]
    public async Task RegisterParticipantAsync_already_durable_does_not_wait_behind_another_sagas_write()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var state = new FakePersistentState<TxRegistryState>();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantsAsync(txid, [3]);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        var other = OnTurnAsync(turn, () => grain.MarkCommittedAsync(Guid.NewGuid()));
        await OnTurnAsync(turn, () => grain.RegisterParticipantAsync(txid, 3));

        Assert.That(other.IsCompleted, Is.False, "The unrelated write must still be outstanding.");
        gate.SetResult();
        await other;
    }

    [Test]
    public async Task SnapshotAsync_during_an_outstanding_write_waits_and_includes_the_durable_decision()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var snapshot = OnTurnAsync(turn, () => grain.SnapshotWithRevisionAsync());
        await DrainAsync(turn);
        Assert.That(snapshot.IsCompleted, Is.False, "A snapshot must not be served from un-durable state.");

        gate.SetResult();
        await mark;
        var result = await snapshot;
        Assert.That(result.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task RecordTerminalArrivalAsync_interleaved_arrivals_report_exactly_one_final_arrival()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var state = new FakePersistentState<TxRegistryState>();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = true;
        state.BeforeWrite = () =>
        {
            if (!gated) return Task.CompletedTask;
            gated = false;
            return gate.Task;
        };

        const int shards = 4;
        var arrivals = Enumerable.Range(0, shards)
            .Select(i => OnTurnAsync(turn, () => grain.RecordTerminalArrivalAsync(txid, i, committed: true, shards)))
            .ToArray();
        await DrainAsync(turn);
        gate.SetResult();
        var results = await Task.WhenAll(arrivals);

        Assert.That(results.Count(r => r.IsFinal), Is.EqualTo(1),
            "Arrivals sharing one write must still observe the tally in arrival order.");
    }

    [Test]
    public void ITxRegistryGrain_interleaves_every_member_except_the_tree_wide_reads()
    {
        string[] nonInterleaved =
        [
            nameof(ITxRegistryGrain.SnapshotAsync),
            nameof(ITxRegistryGrain.SnapshotWithRevisionAsync),
            nameof(ITxRegistryGrain.ObserveCrossTreeInFlightAsync),
        ];

        Assert.Multiple(() =>
        {
            foreach (var method in typeof(ITxRegistryGrain).GetMethods())
            {
                var interleaves = method.GetCustomAttribute<AlwaysInterleaveAttribute>() is not null;
                Assert.That(interleaves, Is.EqualTo(!nonInterleaved.Contains(method.Name)),
                    $"{method.Name}: group commit requires the mutating and per-transaction calls to interleave with an outstanding write.");
            }
        });
    }
}
