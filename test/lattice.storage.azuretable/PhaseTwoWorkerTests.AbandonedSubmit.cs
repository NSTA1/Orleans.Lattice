using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Coverage for a phase-2 submit the worker abandons on its
/// <c>PhaseTwoCommitTimeout</c> (#3458). The caller fails fast at the
/// deadline, but the Azure transaction can still land afterwards, so the
/// worker must keep it running to a real outcome, fence it for the
/// post-failure resync, and account for it if it lands.
/// </summary>
public partial class PhaseTwoWorkerTests
{
    private static readonly TimeSpan ShortCommitTimeout = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// A submit whose first call completes only when the test releases
    /// <see cref="Gate"/>: the shape of an Azure transaction that is
    /// already on the wire when the deadline fires. Cancelling its token
    /// abandons only the local wait, as the SDK does; the gate models the
    /// service-side outcome, which cancellation cannot retract.
    /// </summary>
    private sealed class ZombieSubmit
    {
        public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationToken ObservedToken { get; private set; }

        public int Calls;

        public Task SubmitAsync(TableTransactionAction[] actions, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref Calls) == 1)
            {
                ObservedToken = cancellationToken;
                return Gate.Task.WaitAsync(cancellationToken);
            }

            return Task.CompletedTask;
        }
    }

    private static TableTransactionAction? FindTail(TableTransactionAction[] actions) =>
        actions.FirstOrDefault(a => ((ITableEntity)a.Entity).RowKey == AzureTableWalStorageProvider.TailRowKey);

    [Test]
    public async Task CommitTimeout_does_not_cancel_the_abandoned_submit()
    {
        // Cancelling the SDK call at the deadline completes the local task
        // but cannot retract a request the service has already received,
        // so a fence keyed on that task would release before the
        // transaction's real outcome is known.
        var zombie = new ZombieSubmit();
        var submitter = new RecordingSubmitter(zombie.SubmitAsync);
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
        await Task.Delay(50);

        Assert.That(zombie.ObservedToken.IsCancellationRequested, Is.False,
            "the abandoned transaction must keep running until the transport reports its outcome");
        zombie.Gate.SetResult();
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_that_lands_raises_the_tail_high_water_mark()
    {
        // The zombie wrote TAIL = 4. A later, lower-ended commit must not
        // upsert TAIL beneath it.
        var zombie = new ZombieSubmit();
        var submitter = new RecordingSubmitter(zombie.SubmitAsync);
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
        zombie.Gate.SetResult();
        await worker.AbandonedSubmits.WhenIdleAsync().WaitAsync(TimeSpan.FromSeconds(5));

        await worker.EnqueueAsync(2L, 3L);

        var tail = FindTail(submitter.Calls[1]);
        Assert.That(tail is null || ((AzureTableWalEntity)tail.Entity).Offset >= 4L, Is.True,
            "a commit after a landed zombie must never regress TAIL below the zombie's end offset");
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_that_faults_leaves_the_tail_high_water_mark()
    {
        var zombie = new ZombieSubmit();
        var submitter = new RecordingSubmitter(zombie.SubmitAsync);
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
        zombie.Gate.SetException(new InvalidOperationException("zombie failed"));
        await worker.AbandonedSubmits.WhenIdleAsync().WaitAsync(TimeSpan.FromSeconds(5));

        await worker.EnqueueAsync(2L, 3L);

        var tail = FindTail(submitter.Calls[1]);
        Assert.That(tail, Is.Not.Null, "a failed zombie wrote nothing, so the next commit must advance TAIL");
        Assert.That(((AzureTableWalEntity)tail!.Entity).Offset, Is.EqualTo(3L));
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_holds_the_fence_until_it_lands()
    {
        var zombie = new ZombieSubmit();
        var submitter = new RecordingSubmitter(zombie.SubmitAsync);
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));

        var idle = worker.AbandonedSubmits.WhenIdleAsync();
        await Task.Delay(50);
        Assert.Multiple(() =>
        {
            Assert.That(worker.AbandonedSubmits.Active, Is.EqualTo(1));
            Assert.That(idle.IsCompleted, Is.False, "the fence must hold while the abandoned transaction is in flight");
            Assert.That(worker.OutstandingCommits.Active, Is.Zero, "callers were still failed fast at the deadline");
        });

        zombie.Gate.SetResult();
        await idle.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(worker.AbandonedSubmits.Active, Is.Zero);
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_that_faults_releases_the_fence()
    {
        var zombie = new ZombieSubmit();
        var submitter = new RecordingSubmitter(zombie.SubmitAsync);
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
        Assert.That(worker.AbandonedSubmits.Active, Is.EqualTo(1));

        zombie.Gate.SetException(new InvalidOperationException("zombie failed"));

        await worker.AbandonedSubmits.WhenIdleAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(worker.AbandonedSubmits.Active, Is.Zero);
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_is_cancelled_after_the_settle_cap()
    {
        // A transport that never returns must not hold the fence forever:
        // past the settle cap the worker cancels the call so the fence
        // releases and the resync can proceed.
        CancellationToken observed = default;
        var submitter = new RecordingSubmitter((_, ct) =>
        {
            observed = ct;
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ct.Register(() => tcs.TrySetCanceled(ct));
            return tcs.Task;
        });
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync,
            ManifestPartitionKey,
            TimeSpan.Zero,
            commitTimeout: ShortCommitTimeout,
            abandonedSubmitSettleCap: TimeSpan.FromMilliseconds(100));

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));

        await worker.AbandonedSubmits.WhenIdleAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(observed.IsCancellationRequested, Is.True);
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_fence_releases_on_dispose()
    {
        var submitter = new RecordingSubmitter((_, ct) =>
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            ct.Register(() => tcs.TrySetCanceled(ct));
            return tcs.Task;
        });
        var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero, commitTimeout: ShortCommitTimeout);

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
        Assert.That(worker.AbandonedSubmits.Active, Is.EqualTo(1));

        await worker.DisposeAsync();

        await worker.AbandonedSubmits.WhenIdleAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
