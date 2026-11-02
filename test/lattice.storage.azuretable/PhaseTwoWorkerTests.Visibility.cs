namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Issue #5470: <see cref="PhaseTwoWorker.WhenAcceptedCommitsSettledAsync"/> is
/// the per-shard visibility barrier behind
/// <see cref="AzureTableWalStorageProvider.WaitForAppendVisibilityAsync"/>. It
/// waits for exactly the batches accepted for phase 2 when it is called - the
/// ones a reader cannot see yet - and surfaces their faults.
/// </summary>
public partial class PhaseTwoWorkerTests
{
    [Test]
    public async Task WhenAcceptedCommitsSettledAsync_completes_at_once_with_nothing_accepted()
    {
        var submitter = new RecordingSubmitter();
        await using var worker = NewWorker(submitter);

        Assert.That(worker.WhenAcceptedCommitsSettledAsync().IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task WhenAcceptedCommitsSettledAsync_waits_for_a_batch_whose_manifest_commit_has_not_landed()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var submitter = new RecordingSubmitter((_, _) => release.Task);
        await using var worker = NewWorker(submitter);

        var commit = worker.EnqueueAsync(0L, 38L);
        var barrier = worker.WhenAcceptedCommitsSettledAsync();

        Assert.That(barrier.IsCompleted, Is.False,
            "the batch is durable but not yet in the manifest, so it is not readable");

        release.TrySetResult();
        await barrier.ConfigureAwait(false);
        Assert.That(commit.IsCompletedSuccessfully, Is.True,
            "the barrier completes only once the manifest commit has landed");
    }

    [Test]
    public async Task WhenAcceptedCommitsSettledAsync_does_not_wait_on_a_batch_accepted_after_the_call()
    {
        // The first submit is held open and signals once it is in flight, so
        // the later batch cannot coalesce into it and is a separate commit.
        var calls = 0;
        var firstInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holdSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var submitter = new RecordingSubmitter((_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                firstInFlight.TrySetResult();
                return releaseFirst.Task;
            }
            return holdSecond.Task;
        });
        await using var worker = NewWorker(submitter);

        var first = worker.EnqueueAsync(0L, 99L);
        await firstInFlight.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        var barrier = worker.WhenAcceptedCommitsSettledAsync();
        var later = worker.EnqueueAsync(100L, 138L);
        ObserveFaults(later);

        releaseFirst.TrySetResult();
        await barrier.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        Assert.Multiple(() =>
        {
            Assert.That(first.IsCompletedSuccessfully, Is.True);
            Assert.That(later.IsCompleted, Is.False,
                "a batch accepted after the call must not hold the barrier");
        });

        holdSecond.TrySetResult();
        await later.ConfigureAwait(false);
    }

    [Test]
    public async Task WhenAcceptedCommitsSettledAsync_rethrows_a_phase_two_fault()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var submitter = new RecordingSubmitter(async (_, _) =>
        {
            await release.Task.ConfigureAwait(false);
            throw new InvalidOperationException("phase-2 boom");
        });
        await using var worker = NewWorker(submitter);

        var commit = worker.EnqueueAsync(0L, 38L);
        ObserveFaults(commit);
        var barrier = worker.WhenAcceptedCommitsSettledAsync();
        release.TrySetResult();

        Assert.That(async () => await barrier.ConfigureAwait(false),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("phase-2 boom"),
            "a batch whose manifest commit failed is not readable, so the barrier must not report it as such");
    }
}
