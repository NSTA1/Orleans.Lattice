using System.Collections.Concurrent;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box unit tests for the per-shard <see cref="PhaseTwoWorker"/>
/// (the per-shard phase-2 manifest scheduler). The worker is the strict offset-FIFO,
/// coalescing-up-to-99 scheduler that owns the phase-2 manifest +
/// TAIL writes; integration tests against Azurite already exercise
/// the production constructor end-to-end, so these tests instead
/// pin the in-process invariants the worker is responsible for:
/// strict offset ordering, multi-commit coalescing into one
/// transaction, all-or-nothing failure of the coalesced group plus
/// every later pending commit, TAIL = highest end-offset in the
/// committed group, and disposal that faults pending commits.
/// </summary>
[TestFixture]
public class PhaseTwoWorkerTests
{
    private const string ManifestPartitionKey = "_m_|tree|0";

    /// <summary>
    /// Records every batch of <see cref="TableTransactionAction"/>
    /// the worker submits, in submission order. Each recorded entry
    /// is a fresh array so the test does not race the worker on the
    /// underlying list capacity.
    /// </summary>
    private sealed class RecordingSubmitter
    {
        private readonly ConcurrentQueue<TableTransactionAction[]> _calls = new();
        private readonly Func<TableTransactionAction[], CancellationToken, Task>? _override;

        public RecordingSubmitter(Func<TableTransactionAction[], CancellationToken, Task>? perCallOverride = null)
        {
            _override = perCallOverride;
        }

        public IReadOnlyList<TableTransactionAction[]> Calls => _calls.ToArray();

        public Task SubmitAsync(IReadOnlyList<TableTransactionAction> actions, CancellationToken cancellationToken)
        {
            var snapshot = actions.ToArray();
            _calls.Enqueue(snapshot);
            return _override is null
                ? Task.CompletedTask
                : _override(snapshot, cancellationToken);
        }
    }

    private static PhaseTwoWorker NewWorker(RecordingSubmitter submitter) =>
        new(submitter.SubmitAsync, ManifestPartitionKey);

    private static async Task WaitForCallsAsync(RecordingSubmitter submitter, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            if (submitter.Calls.Count >= expected)
            {
                return;
            }
            await Task.Delay(5).ConfigureAwait(false);
        }
        Assert.Fail($"Expected at least {expected} submit calls within 2s, observed {submitter.Calls.Count}.");
    }

    [Test]
    public async Task EnqueueAsync_single_commit_emits_one_manifest_row_and_tail()
    {
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        await worker.EnqueueAsync(0L, 4L).ConfigureAwait(false);

        Assert.That(submitter.Calls.Count, Is.EqualTo(1));
        var actions = submitter.Calls[0];

        Assert.Multiple(() =>
        {
            Assert.That(actions.Length, Is.EqualTo(3), "1 candidate-row delete + 1 manifest row add + 1 TAIL upsert");
            Assert.That(actions[0].ActionType, Is.EqualTo(TableTransactionActionType.Delete));
            Assert.That(((AzureTableWalEntity)actions[0].Entity).PartitionKey, Is.EqualTo(ManifestPartitionKey));
            Assert.That(((AzureTableWalEntity)actions[0].Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.BuildCandidateRowKey(0L)));
            Assert.That(actions[1].ActionType, Is.EqualTo(TableTransactionActionType.Add));
            Assert.That(((AzureTableWalEntity)actions[1].Entity).PartitionKey, Is.EqualTo(ManifestPartitionKey));
            Assert.That(((AzureTableWalEntity)actions[1].Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.BuildManifestRowKey(0L)));
            Assert.That(((AzureTableWalEntity)actions[1].Entity).Offset, Is.EqualTo(4L));
            Assert.That(actions[2].ActionType, Is.EqualTo(TableTransactionActionType.UpsertReplace));
            Assert.That(((AzureTableWalEntity)actions[2].Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.TailRowKey));
            Assert.That(((AzureTableWalEntity)actions[2].Entity).Offset, Is.EqualTo(4L), "TAIL = endOffsetInclusive of the only commit");
        });
    }

    [Test]
    public async Task EnqueueAsync_completes_only_after_submit_durably_returns()
    {
        // Submitter that suspends until released so the test can observe
        // the worker's TCS staying incomplete until the submit promise
        // resolves.
        var release = new TaskCompletionSource();
        var submitter = new RecordingSubmitter((actions, ct) => release.Task);
        await using var worker = NewWorker(submitter);

        var enqueue = worker.EnqueueAsync(0L, 0L);

        await Task.Delay(50).ConfigureAwait(false);
        Assert.That(enqueue.IsCompleted, Is.False, "EnqueueAsync must not complete until the submit returns");

        release.SetResult();
        await enqueue.ConfigureAwait(false);
        Assert.That(enqueue.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task EnqueueAsync_in_arrival_order_commits_one_at_a_time_when_arrivals_are_serialised()
    {
        // Serialised arrivals (await between EnqueueAsync calls) - each
        // commit drains the channel into the sorted set on its own
        // iteration so they fire as N separate submits.
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        await worker.EnqueueAsync(0L, 4L).ConfigureAwait(false);
        await worker.EnqueueAsync(5L, 9L).ConfigureAwait(false);
        await worker.EnqueueAsync(10L, 14L).ConfigureAwait(false);

        Assert.That(submitter.Calls.Count, Is.EqualTo(3));
        Assert.That(
            submitter.Calls.Select(c => ((AzureTableWalEntity)c[^1].Entity).Offset),
            Is.EqualTo(new long[] { 4L, 9L, 14L }),
            "TAIL must climb monotonically across the three submits");
    }

    [Test]
    public async Task EnqueueAsync_out_of_order_arrivals_commit_in_ascending_start_offset_order()
    {
        // Block the first commit so a burst of out-of-order arrivals
        // accumulates in the sorted set behind it, then release the
        // gate and observe that the post-gate batch fires in ascending
        // start-offset order.
        var gate = new TaskCompletionSource();
        var callCount = 0;
        var submitter = new RecordingSubmitter((actions, ct) =>
        {
            // Gate only the first submit; subsequent submits return
            // immediately so the worker drains the pile under test.
            if (Interlocked.Increment(ref callCount) == 1)
            {
                return gate.Task;
            }
            return Task.CompletedTask;
        });
        await using var worker = NewWorker(submitter);

        // Prime: send a single low-offset commit and wait until the
        // submitter is actually inside the gated call. The worker
        // drains the channel into the sorted set before invoking
        // submit, so once the first submit is in-flight, every
        // subsequent enqueue lands in the sorted set behind it.
        var firstTask = worker.EnqueueAsync(0L, 4L);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);

        // Burst: enqueue in reverse-offset order while the submitter
        // is parked. The worker buffers them all.
        var t30 = worker.EnqueueAsync(30L, 34L);
        var t20 = worker.EnqueueAsync(20L, 24L);
        var t10 = worker.EnqueueAsync(10L, 14L);

        // Release. The worker now drains the sorted set in ascending
        // order. Could coalesce all three into one submit OR fire them
        // in two submits depending on race timing - the invariant is
        // start-offset order, not call count.
        gate.SetResult();

        await Task.WhenAll(firstTask, t10, t20, t30).ConfigureAwait(false);

        // Concatenate every M-row across every submit (excluding the
        // priming submit) and verify ascending start-offset order.
        var startOffsets = submitter.Calls
            .Skip(1)
            .SelectMany(c => c.Where(a => a.ActionType == TableTransactionActionType.Add))
            .Select(a => long.Parse(
                ((AzureTableWalEntity)a.Entity).RowKey.AsSpan(AzureTableWalStorageProvider.ManifestRowKeyPrefix.Length)))
            .ToArray();

        Assert.That(startOffsets, Is.EqualTo(new long[] { 10L, 20L, 30L }), "manifest rows must be emitted in ascending start-offset order");

        // TAIL of the final submit must equal the highest endOffsetInclusive (34).
        var lastTail = ((AzureTableWalEntity)submitter.Calls[^1][^1].Entity).Offset;
        Assert.That(lastTail, Is.EqualTo(34L), "TAIL of the last submit must equal the highest endOffsetInclusive");
    }

    [Test]
    public async Task EnqueueAsync_high_offset_committed_first_does_not_regress_when_a_lower_offset_arrives_later()
    {
        // Concurrent same-shard appends whose phase 0/1 races
        // complete out of start-offset order arrive at the worker
        // in the "wrong" order. The drain loop restores ascending
        // start-offset order across the *pending set*, but a
        // high-offset arrival that lands first can still be
        // committed in its own phase-2 transaction before the
        // lower-offset arrival reaches the worker. In that case
        // TAIL must NOT regress: the second commit either skips
        // the TAIL upsert or upserts the same higher value.
        //
        // This is the regression test for the failure
        //   AppendBatchAsync_concurrent_distinct_batches_into_one_shard_all_persist
        // surfaced where TAIL on disk ended up at 1 instead of 4
        // after batches (0,1) and (2,3,4) raced against a shared
        // shard.
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        // Force two SEPARATE phase-2 transactions in reverse
        // start-offset order by awaiting between enqueues.
        await worker.EnqueueAsync(2L, 4L).ConfigureAwait(false);
        await worker.EnqueueAsync(0L, 1L).ConfigureAwait(false);

        Assert.That(submitter.Calls.Count, Is.EqualTo(2), "two separate commits expected when arrivals are serialised");

        // First submit commits (2,4): C-del + M-add + TAIL=4.
        var firstTail = submitter.Calls[0]
            .Where(a => a.ActionType == TableTransactionActionType.UpsertReplace
                && ((AzureTableWalEntity)a.Entity).RowKey == AzureTableWalStorageProvider.TailRowKey)
            .Select(a => ((AzureTableWalEntity)a.Entity).Offset)
            .Single();
        Assert.That(firstTail, Is.EqualTo(4L), "first commit upserts TAIL to its endOffsetInclusive");

        // Second submit commits (0,1): C-del + M-add. TAIL must
        // either be absent from the transaction (the worker
        // recognises the upsert would regress and skips it) or
        // explicitly clamped to >= 4. Either way the final
        // persisted TAIL must be 4, not 1.
        var secondTailActions = submitter.Calls[1]
            .Where(a => a.ActionType == TableTransactionActionType.UpsertReplace
                && ((AzureTableWalEntity)a.Entity).RowKey == AzureTableWalStorageProvider.TailRowKey)
            .Select(a => ((AzureTableWalEntity)a.Entity).Offset)
            .ToArray();

        if (secondTailActions.Length > 0)
        {
            Assert.That(secondTailActions[0], Is.GreaterThanOrEqualTo(4L),
                "if the second commit upserts TAIL it must not regress below the first commit's TAIL");
        }

        // M-rows must still be emitted in ascending start-offset
        // order across the two transactions even though the
        // arrivals were reversed.
        var mRowOffsets = submitter.Calls
            .SelectMany(c => c.Where(a => a.ActionType == TableTransactionActionType.Add))
            .Select(a => long.Parse(
                ((AzureTableWalEntity)a.Entity).RowKey.AsSpan(AzureTableWalStorageProvider.ManifestRowKeyPrefix.Length)))
            .ToArray();
        Assert.That(mRowOffsets, Is.EqualTo(new long[] { 2L, 0L }),
            "M-rows are emitted in arrival-commit order; the test pins that two separate commits occurred "
            + "(arrival sequence 2-then-0), which is exactly the race that exposed the TAIL regression bug");
    }

    [Test]
    public async Task EnqueueAsync_coalesces_many_pending_commits_into_one_submit()
    {
        // Park the submitter on the first call so a backlog of 49
        // arrivals piles up, then release; the worker should drain
        // them all in a single coalesced submit (49 C-deletes + 49
        // M-adds + 1 TAIL upsert = 99 actions, just under the
        // 100-action Azure Tables transaction cap).
        var gate = new TaskCompletionSource();
        var primed = 0;
        var submitter = new RecordingSubmitter((actions, ct) =>
        {
            if (Interlocked.Increment(ref primed) == 1)
            {
                return gate.Task;
            }
            return Task.CompletedTask;
        });
        await using var worker = NewWorker(submitter);

        // Prime call #1 - the gated one.
        var priming = worker.EnqueueAsync(0L, 0L);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);

        // Now queue 49 more in-order commits behind the gate.
        var tasks = new List<Task>(49);
        for (var i = 1; i <= 49; i++)
        {
            tasks.Add(worker.EnqueueAsync(i, i));
        }

        gate.SetResult();
        await Task.WhenAll(tasks).ConfigureAwait(false);
        await priming.ConfigureAwait(false);

        // Exactly two submits: the primed one (1 C-delete + 1 M-add
        // + TAIL = 3 actions) and the coalesced one (49 C-deletes +
        // 49 M-adds + TAIL = 99 actions).
        Assert.That(submitter.Calls.Count, Is.EqualTo(2));
        var coalesced = submitter.Calls[1];
        Assert.Multiple(() =>
        {
            Assert.That(coalesced.Length, Is.EqualTo(99), "49 C-deletes + 49 M-adds + 1 TAIL upsert in one transaction");
            Assert.That(coalesced[^1].ActionType, Is.EqualTo(TableTransactionActionType.UpsertReplace));
            Assert.That(((AzureTableWalEntity)coalesced[^1].Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.TailRowKey));
            Assert.That(((AzureTableWalEntity)coalesced[^1].Entity).Offset, Is.EqualTo(49L), "TAIL = highest endOffsetInclusive in coalesced group");
        });
    }

    [Test]
    public async Task EnqueueAsync_coalesces_at_most_49_commits_per_submit()
    {
        // Same as the previous test but with 75 backlogged commits;
        // the worker must split into a 49-batch submit and a 26-batch
        // submit (never exceed the 49-batch coalescing cap so the
        // total action count stays under the 100-action transaction
        // cap: 49 * 2 + 1 = 99).
        var gate = new TaskCompletionSource();
        var primed = 0;
        var submitter = new RecordingSubmitter((actions, ct) =>
        {
            if (Interlocked.Increment(ref primed) == 1)
            {
                return gate.Task;
            }
            return Task.CompletedTask;
        });
        await using var worker = NewWorker(submitter);

        var priming = worker.EnqueueAsync(0L, 0L);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);

        var tasks = new List<Task>(75);
        for (var i = 1; i <= 75; i++)
        {
            tasks.Add(worker.EnqueueAsync(i, i));
        }

        gate.SetResult();
        await Task.WhenAll(tasks).ConfigureAwait(false);
        await priming.ConfigureAwait(false);

        // Every submit after the priming call must hold <= 100
        // actions (at most 49 C-deletes + 49 M-adds + 1 TAIL). The
        // 75-commit backlog must therefore fan out across at least
        // two coalesced submits.
        var coalescedSubmits = submitter.Calls.Skip(1).ToArray();
        Assert.That(coalescedSubmits.Length, Is.GreaterThanOrEqualTo(2), "75 backlogged commits cannot fit in a single 49-batch coalesced submit");
        Assert.That(coalescedSubmits.All(c => c.Length <= 100), Is.True, "no submit may exceed the 100-action transaction cap");

        // Sum of M-row adds across the coalesced submits must equal
        // 75 (one M-add per committed batch).
        var mRowCount = coalescedSubmits.Sum(c => c.Count(a => a.ActionType == TableTransactionActionType.Add));
        Assert.That(mRowCount, Is.EqualTo(75), "every backlogged commit must be reflected in exactly one M-row across the coalesced submits");

        // Same count of C-row deletes - one delete per committed
        // batch, paired 1:1 with the M-add.
        var cRowDeletes = coalescedSubmits.Sum(c => c.Count(a => a.ActionType == TableTransactionActionType.Delete));
        Assert.That(cRowDeletes, Is.EqualTo(75), "every committed batch must remove its phase-0 candidate-row");
    }

    [Test]
    public async Task EnqueueAsync_failure_faults_every_commit_in_the_coalesced_group()
    {
        // Submit always faults. Four commits arrive close enough in
        // time that the worker drains them all into the sorted set in
        // one iteration, coalesces them into one transaction, and the
        // transaction fault propagates to every TCS in the group.
        var failure = new InvalidOperationException("simulated phase-2 failure");
        var submitter = new RecordingSubmitter((actions, ct) => Task.FromException(failure));
        await using var worker = NewWorker(submitter);

        // Burst the four arrivals without awaiting any of them
        // individually so they're all in the channel before the drain
        // loop's first WaitToReadAsync returns. TryRead then pulls all
        // four into _pending in one iteration, CommitBatchAsync
        // coalesces them, and the failure fans out across the group.
        var t0 = worker.EnqueueAsync(0L, 0L);
        var t1 = worker.EnqueueAsync(1L, 1L);
        var t2 = worker.EnqueueAsync(2L, 2L);
        var t3 = worker.EnqueueAsync(3L, 3L);

        // At least one of them faults (whichever became the in-flight
        // batch). The others either fault in the same batch or fault
        // because they were pending behind it - either way every TCS
        // must observe the failure.
        await AssertEventuallyFaultsAsync(t0, failure);
        await AssertEventuallyFaultsAsync(t1, failure);
        await AssertEventuallyFaultsAsync(t2, failure);
        await AssertEventuallyFaultsAsync(t3, failure);
    }

    [Test]
    public async Task EnqueueAsync_after_failure_continues_to_drain_new_arrivals()
    {
        // After a sticky-failure round, new arrivals should still be
        // processed (the worker fault-propagates to existing pending
        // commits then clears the buffer and keeps looping).
        var primed = 0;
        var submitter = new RecordingSubmitter((actions, ct) =>
        {
            if (Interlocked.Increment(ref primed) == 1)
            {
                throw new InvalidOperationException("first attempt always fails");
            }
            return Task.CompletedTask;
        });
        await using var worker = NewWorker(submitter);

        var failed = worker.EnqueueAsync(0L, 0L);
        try
        {
            await failed.ConfigureAwait(false);
            Assert.Fail("expected the first commit to fault");
        }
        catch (InvalidOperationException)
        {
            // expected
        }

        // New commit must drain and succeed.
        await worker.EnqueueAsync(1L, 1L).ConfigureAwait(false);
        Assert.That(submitter.Calls.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task DisposeAsync_faults_in_flight_and_pending_commits()
    {
        // Park the in-flight submit on the worker's cancellation token;
        // queue a backlog; dispose; every commit (in-flight + backlog)
        // must fault rather than hang. The worker propagates the
        // cancellation through CommitBatchAsync's catch block, so the
        // resulting fault is OperationCanceledException; the
        // ObjectDisposedException path in the drain loop's finally
        // block is a defence-in-depth fallback for arrivals that never
        // entered the sorted set.
        var submitter = new RecordingSubmitter((actions, ct) =>
        {
            var tcs = new TaskCompletionSource();
            ct.Register(() => tcs.TrySetCanceled(ct));
            return tcs.Task;
        });
        var worker = NewWorker(submitter);

        var inFlight = worker.EnqueueAsync(0L, 0L);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);
        var pending1 = worker.EnqueueAsync(1L, 1L);
        var pending2 = worker.EnqueueAsync(2L, 2L);

        // Let the new arrivals settle in the channel before we dispose.
        await Task.Delay(50).ConfigureAwait(false);

        await worker.DisposeAsync().ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(inFlight.IsFaulted || inFlight.IsCanceled, Is.True, "in-flight commit must not hang on dispose");
            Assert.That(pending1.IsFaulted || pending1.IsCanceled, Is.True, "pending commit 1 must not hang on dispose");
            Assert.That(pending2.IsFaulted || pending2.IsCanceled, Is.True, "pending commit 2 must not hang on dispose");
        });
    }

    [Test]
    public async Task DisposeAsync_is_idempotent()
    {
        var submitter = new RecordingSubmitter();
        var worker = NewWorker(submitter);
        await worker.EnqueueAsync(0L, 0L).ConfigureAwait(false);

        await worker.DisposeAsync().ConfigureAwait(false);
        Assert.DoesNotThrowAsync(async () => await worker.DisposeAsync().ConfigureAwait(false));
    }

    [Test]
    public async Task EnqueueAsync_after_dispose_faults_with_object_disposed_exception()
    {
        var submitter = new RecordingSubmitter();
        var worker = NewWorker(submitter);
        await worker.DisposeAsync().ConfigureAwait(false);

        var task = worker.EnqueueAsync(0L, 0L);
        try
        {
            await task.ConfigureAwait(false);
            Assert.Fail("expected ObjectDisposedException");
        }
        catch (ObjectDisposedException)
        {
            // expected
        }
    }

    [Test]
    public async Task EnqueueAsync_emits_manifest_row_keys_matching_the_provider_helper()
    {
        // No schema drift: the worker's M-row keys must exactly match
        // what AzureTableWalStorageProvider.BuildManifestRowKey
        // produces, otherwise reads (which point-read using the same
        // helper) would miss the row.
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        await worker.EnqueueAsync(42L, 99L).ConfigureAwait(false);

        // Find the M-row (the Add action - the Delete is the C-row,
        // the UpsertReplace is TAIL) and assert its row key.
        var mRow = submitter.Calls[0].First(a => a.ActionType == TableTransactionActionType.Add);
        Assert.That(((AzureTableWalEntity)mRow.Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.BuildManifestRowKey(42L)));

        // And the candidate-row delete must use the provider's
        // candidate-row helper.
        var cRow = submitter.Calls[0].First(a => a.ActionType == TableTransactionActionType.Delete);
        Assert.That(((AzureTableWalEntity)cRow.Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.BuildCandidateRowKey(42L)));
    }

    [Test]
    public async Task EnqueueAsync_with_hasCandidateRow_false_emits_only_manifest_row_and_tail()
    {
        // Variant D contract: when the originating AppendBatchAsync
        // ran with EliminateCandidateRowOnHotPath = true, no C-row
        // was ever written, so the worker MUST NOT emit a Delete for
        // a non-existent row (which would fail the whole transaction
        // with HTTP 404). The transaction shrinks to M-add + TAIL.
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        await worker.EnqueueAsync(0L, 4L, hasCandidateRow: false).ConfigureAwait(false);

        Assert.That(submitter.Calls.Count, Is.EqualTo(1));
        var actions = submitter.Calls[0];

        Assert.Multiple(() =>
        {
            Assert.That(actions.Length, Is.EqualTo(2), "D-mode commit: 1 manifest row add + 1 TAIL upsert, no C-row delete");
            Assert.That(actions.Any(a => a.ActionType == TableTransactionActionType.Delete), Is.False,
                "C-row delete must be elided when hasCandidateRow=false");
            Assert.That(actions[0].ActionType, Is.EqualTo(TableTransactionActionType.Add));
            Assert.That(((AzureTableWalEntity)actions[0].Entity).RowKey,
                Is.EqualTo(AzureTableWalStorageProvider.BuildManifestRowKey(0L)));
            Assert.That(((AzureTableWalEntity)actions[0].Entity).Offset, Is.EqualTo(4L));
            Assert.That(actions[1].ActionType, Is.EqualTo(TableTransactionActionType.UpsertReplace));
            Assert.That(((AzureTableWalEntity)actions[1].Entity).RowKey, Is.EqualTo(AzureTableWalStorageProvider.TailRowKey));
            Assert.That(((AzureTableWalEntity)actions[1].Entity).Offset, Is.EqualTo(4L));
        });
    }

    [Test]
    public async Task EnqueueAsync_coalesces_mixed_candidate_row_flags_within_one_submit()
    {
        // Heterogeneity contract: a coalesced phase-2 transaction
        // may carry both legacy (HasCandidateRow=true) and D-mode
        // (HasCandidateRow=false) commits - e.g. during a rolling
        // toggle of the option, or when reconciliation rolls a
        // legacy orphan forward in the same window as a fresh
        // D-mode append. The worker must emit a C-delete ONLY for
        // the legacy commits and an M-add for every commit.
        var gate = new TaskCompletionSource();
        var primed = 0;
        var submitter = new RecordingSubmitter((_, _) =>
        {
            if (Interlocked.Increment(ref primed) == 1)
            {
                return gate.Task;
            }
            return Task.CompletedTask;
        });
        await using var worker = NewWorker(submitter);

        // Prime the first submit so subsequent enqueues coalesce.
        var priming = worker.EnqueueAsync(0L, 0L, hasCandidateRow: true);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);

        // Three commits behind the gate: mixed flags.
        var t1 = worker.EnqueueAsync(1L, 1L, hasCandidateRow: false);
        var t2 = worker.EnqueueAsync(2L, 2L, hasCandidateRow: true);
        var t3 = worker.EnqueueAsync(3L, 3L, hasCandidateRow: false);

        gate.SetResult();
        await Task.WhenAll(priming, t1, t2, t3).ConfigureAwait(false);

        // The priming call's submit had exactly one commit; the
        // coalesced submit has three.
        Assert.That(submitter.Calls.Count, Is.EqualTo(2));
        var coalesced = submitter.Calls[1];

        var deletes = coalesced
            .Where(a => a.ActionType == TableTransactionActionType.Delete)
            .Select(a => ((AzureTableWalEntity)a.Entity).RowKey)
            .ToArray();
        var adds = coalesced
            .Where(a => a.ActionType == TableTransactionActionType.Add)
            .Select(a => ((AzureTableWalEntity)a.Entity).RowKey)
            .ToArray();

        Assert.Multiple(() =>
        {
            // Only offset 2L had hasCandidateRow=true, so exactly one
            // C-row delete is expected.
            Assert.That(deletes, Is.EqualTo(new[]
            {
                AzureTableWalStorageProvider.BuildCandidateRowKey(2L),
            }), "C-delete must be emitted only for HasCandidateRow=true commits");

            // M-add fires for every commit in ascending start-offset.
            Assert.That(adds, Is.EqualTo(new[]
            {
                AzureTableWalStorageProvider.BuildManifestRowKey(1L),
                AzureTableWalStorageProvider.BuildManifestRowKey(2L),
                AzureTableWalStorageProvider.BuildManifestRowKey(3L),
            }));

            // TAIL of the coalesced submit upserts to the highest
            // endOffsetInclusive across the group.
            var tailRow = coalesced.Single(a =>
                a.ActionType == TableTransactionActionType.UpsertReplace &&
                ((AzureTableWalEntity)a.Entity).RowKey == AzureTableWalStorageProvider.TailRowKey);
            Assert.That(((AzureTableWalEntity)tailRow.Entity).Offset, Is.EqualTo(3L));
        });
    }

    /// <summary>
    /// Awaits <paramref name="task"/> and asserts it faults with
    /// <paramref name="expected"/>. Accepts either reference-equality
    /// or equal-by-(type, message) so the worker may surface either
    /// the original submit-delegate exception or a wrapper carrying
    /// the same message (as happens when the same failure fans out
    /// across multiple coalesced groups during a burst).
    /// </summary>
    private static async Task AssertEventuallyFaultsAsync(Task task, Exception expected)
    {
        try
        {
            await task.ConfigureAwait(false);
            Assert.Fail($"task should have faulted with {expected.GetType().Name}");
        }
        catch (Exception actual)
            when (ReferenceEquals(actual, expected) ||
                  (actual.GetType() == expected.GetType() && actual.Message == expected.Message))
        {
            // expected
        }
        catch (Exception actual)
        {
            Assert.Fail($"task faulted with {actual.GetType().Name}: {actual.Message}, expected {expected.GetType().Name}: {expected.Message}");
        }
    }
}
