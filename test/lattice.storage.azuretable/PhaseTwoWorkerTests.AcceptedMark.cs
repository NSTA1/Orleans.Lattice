namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the <see cref="PhaseTwoWorker"/>'s accepted-range
/// set - the in-memory notion of "phase-1 durable but not yet in the
/// manifest" that
/// <see cref="AzureTableWalStorageProvider.GetHighestOffsetAsync"/>
/// folds over the persisted <c>TAIL</c> so a completed append is never
/// reported back as an offset lower than the one it returned
/// (issue #2528).
/// <para>
/// These tests are deterministic without any timing: the range is
/// recorded <i>synchronously</i> inside
/// <see cref="PhaseTwoWorker.EnqueueAsync"/>, so a test can assert on
/// it the instant the call returns, without awaiting the commit and
/// without a sleep. Where a test needs the commit held off entirely it
/// uses a long coalescing window, which parks the drain loop before it
/// can submit; where it needs a fault it awaits the returned task,
/// which is the fault's own completion signal.
/// </para>
/// </summary>
public partial class PhaseTwoWorkerTests
{
    /// <summary>
    /// Coalescing window long enough that the drain loop parks before
    /// ever submitting, so a test observes the accepted ranges while
    /// the batches are still genuinely pending phase 2.
    /// </summary>
    private static readonly TimeSpan NeverCommitsWindow = TimeSpan.FromMinutes(10);

    private static PhaseTwoWorker NewParkedWorker(RecordingSubmitter submitter) =>
        new(submitter.SubmitAsync, ManifestPartitionKey, NeverCommitsWindow, commitTimeout: null);

    /// <summary>
    /// Observes the faults of tasks a test deliberately leaves pending
    /// so disposal's <see cref="ObjectDisposedException"/> does not
    /// resurface later as an unobserved task exception.
    /// </summary>
    private static void ObserveFaults(params Task[] tasks)
    {
        foreach (var task in tasks)
        {
            _ = task.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_returns_the_persisted_tail_when_nothing_is_accepted()
    {
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        Assert.Multiple(() =>
        {
            Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(-1L),
                "an empty shard with nothing accepted must still report -1");
            Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(99L), Is.EqualTo(99L),
                "a worker that has accepted nothing must never move the persisted answer");
        });
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_advances_before_the_phase_two_commit_lands()
    {
        // The whole point of the fold: the batch is durable (phase 1
        // committed before the worker was ever told about it) but its
        // manifest row and TAIL upsert have not been written, so
        // nothing has been submitted yet.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var pending = worker.EnqueueAsync(0L, 3L);
        ObserveFaults(pending);

        Assert.Multiple(() =>
        {
            Assert.That(submitter.Calls.Count, Is.EqualTo(0),
                "the drain loop must still be parked in the coalescing window");
            Assert.That(pending.IsCompleted, Is.False,
                "the phase-2 commit must not have landed");
            Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(3L),
                "the batch must be vouched for as soon as it is queued");
        });
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_extends_a_non_zero_persisted_tail()
    {
        // The worker is created lazily per shard, so a shard whose
        // TAIL is already well past zero must still have its accepted
        // batches folded on top of that value rather than replacing it.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var pending = worker.EnqueueAsync(100L, 104L);
        ObserveFaults(pending);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(99L), Is.EqualTo(104L));
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_does_not_advance_across_a_gap()
    {
        // Phase 1 completes out of order, so a higher batch can be
        // accepted while a lower one is still in flight. Reporting the
        // maximum there would assert durability across a hole that
        // does not exist yet - and would diverge from recovery, which
        // rolls an orphan above a gap back rather than forward.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var first = worker.EnqueueAsync(0L, 3L);
        var aboveGap = worker.EnqueueAsync(8L, 11L);
        ObserveFaults(first, aboveGap);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(3L),
            "offsets 4..7 have not been accepted, so the answer must stop below the hole");
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_does_not_advance_when_the_lowest_batch_is_above_the_tail()
    {
        // The out-of-order case that makes a self-seeded high-water
        // mark unsafe: the very first batch this lazily-created worker
        // sees is not the lowest one outstanding, so it must not be
        // treated as contiguous with the persisted TAIL.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var aboveGap = worker.EnqueueAsync(8L, 11L);
        ObserveFaults(aboveGap);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(3L), Is.EqualTo(3L),
            "offsets 4..7 are still in phase 1, so nothing above them may be reported");
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_advances_over_the_whole_run_once_a_gap_is_filled()
    {
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var first = worker.EnqueueAsync(0L, 3L);
        var aboveGap = worker.EnqueueAsync(8L, 11L);
        var fillsGap = worker.EnqueueAsync(4L, 7L);
        ObserveFaults(first, aboveGap, fillsGap);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(11L),
            "filling the hole must walk the answer over every batch the fill made contiguous");
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_walks_several_out_of_order_batches_in_one_pass()
    {
        // Arrivals in fully reverse order: nothing is reachable from
        // the tail until the lowest batch lands, which must then make
        // the whole run contiguous at once.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var third = worker.EnqueueAsync(12L, 15L);
        var second = worker.EnqueueAsync(8L, 11L);
        var fourth = worker.EnqueueAsync(4L, 7L);
        ObserveFaults(third, second, fourth);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(-1L),
            "nothing is contiguous with the tail until the lowest batch arrives");

        var lowest = worker.EnqueueAsync(0L, 3L);
        ObserveFaults(lowest);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(15L),
            "the lowest batch must make the whole run reachable in one pass");
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_ignores_ranges_already_covered_by_the_tail()
    {
        // A range wholly below TAIL has already been committed and is
        // simply awaiting pruning; it must neither block the walk nor
        // drag the answer backwards.
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var committed = worker.EnqueueAsync(0L, 3L);
        var pending = worker.EnqueueAsync(4L, 7L);
        ObserveFaults(committed, pending);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(3L), Is.EqualTo(7L));
    }

    [Test]
    public async Task ContiguousAcceptedEndOffsetInclusive_never_reports_below_the_persisted_tail()
    {
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var pending = worker.EnqueueAsync(0L, 3L);
        ObserveFaults(pending);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(500L), Is.EqualTo(500L),
            "the fold may only ever extend the persisted value, never lower it");
    }

    [Test]
    public async Task A_failed_phase_two_commit_stops_the_worker_vouching_for_the_faulted_batch()
    {
        // Sticky-failure model: the in-flight batch and every later
        // pending one are faulted and the producer resyncs from the
        // persisted TAIL, so the worker can no longer vouch for
        // anything it has not committed.
        var fail = false;
        var submitter = new RecordingSubmitter((_, _) => fail
            ? Task.FromException(new InvalidOperationException("phase-2 boom"))
            : Task.CompletedTask);
        await using var worker = NewWorker(submitter);

        await worker.EnqueueAsync(0L, 3L).ConfigureAwait(false);

        fail = true;
        var doomed = worker.EnqueueAsync(4L, 7L);
        Assert.That(async () => await doomed.ConfigureAwait(false),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(3L), Is.EqualTo(3L),
            "the faulted batch must be discarded, leaving only the committed tail");
    }

    [Test]
    public async Task The_worker_vouches_again_for_batches_accepted_after_a_fault_reset()
    {
        // The second submit is held open so the re-accepted batch is
        // observed while it is genuinely pending phase 2, rather than
        // after a commit has already pruned it.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var submitter = new RecordingSubmitter((_, _) =>
            Interlocked.Increment(ref calls) == 1
                ? Task.FromException(new InvalidOperationException("phase-2 boom"))
                : release.Task);
        await using var worker = NewWorker(submitter);

        var doomed = worker.EnqueueAsync(100L, 104L);
        Assert.That(async () => await doomed.ConfigureAwait(false),
            Throws.InstanceOf<InvalidOperationException>());
        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(99L), Is.EqualTo(99L),
            "precondition: the fault discarded the accepted range");

        var resynced = worker.EnqueueAsync(100L, 104L);
        ObserveFaults(resynced);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(99L), Is.EqualTo(104L),
            "the post-resync batch must be vouched for again");

        release.TrySetResult();
    }

    [Test]
    public async Task DisposeAsync_stops_the_worker_vouching_for_commits_it_faults()
    {
        // Disposal faults everything still parked, so those offsets
        // stop being vouched for on exactly the commit-failure path.
        var submitter = new RecordingSubmitter();
        var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, NeverCommitsWindow, commitTimeout: null);

        var parked = worker.EnqueueAsync(0L, 3L);
        ObserveFaults(parked);
        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(3L),
            "precondition: the parked batch is vouched for while it is pending");

        await worker.DisposeAsync().ConfigureAwait(false);

        Assert.Multiple(() =>
        {
            Assert.That(submitter.Calls.Count, Is.EqualTo(0),
                "the batch never reached a submit, so nothing was committed");
            Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(-1L),
                "a batch faulted by disposal must not stay vouched for");
        });
    }

    [Test]
    public async Task Committed_ranges_are_pruned_so_the_accepted_set_stays_bounded()
    {
        // The set must not grow with every batch the worker ever sees;
        // a committed range is already reflected in TAIL and is dropped
        // when phase 2 lands. Observed through behaviour: an unpruned
        // range would still be walkable from a tail below it.
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        for (var start = 0L; start < 40L; start += 4L)
        {
            await worker.EnqueueAsync(start, start + 3L).ConfigureAwait(false);
        }

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L), Is.EqualTo(-1L),
            "every range committed, so none may remain to walk from an empty tail");
    }

    [Test]
    public async Task The_accepted_set_survives_concurrent_enqueues_from_many_threads()
    {
        // The enqueue path has many concurrent writers (the channel is
        // created with SingleWriter = false), so the bookkeeping must
        // be gated. Every batch is contiguous, so whatever order the
        // threads win in, the walk must cover the whole run exactly.
        // Kept below MaxBatchedManifestRows so the drain loop parks in
        // the coalescing window instead of short-circuiting it and
        // committing (which would prune the ranges under the test).
        const int batches = 40;
        const int entriesPerBatch = 4;
        var submitter = new RecordingSubmitter();
        await using var worker = NewParkedWorker(submitter);

        var pending = new Task[batches];
        var starts = Enumerable.Range(0, batches).Select(i => (long)i * entriesPerBatch).ToArray();
        Parallel.ForEach(starts, start =>
        {
            pending[(int)(start / entriesPerBatch)] =
                worker.EnqueueAsync(start, start + entriesPerBatch - 1);
        });
        ObserveFaults(pending);

        Assert.That(worker.ContiguousAcceptedEndOffsetInclusive(-1L),
            Is.EqualTo((long)(batches * entriesPerBatch) - 1),
            "every batch is contiguous, so the walk must cover the whole run regardless of arrival order");
    }
}
