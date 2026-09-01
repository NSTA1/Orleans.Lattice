using System.Diagnostics.Metrics;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the residual fault paths of
/// <see cref="PhaseTwoWorker"/> that the primary suite does not reach:
/// the drain-loop shutdown finally faulting commits still parked in the
/// coalescing window, the commit catch-block faulting later still-pending
/// commits with a <see cref="RequestFailedException"/> (exercising the
/// SDK-status arm of the status-tag resolver), and the abandoned-submit
/// observer running against a genuinely faulted (not merely cancelled)
/// submit task. These pin the worker's all-or-nothing failure and
/// resource-hygiene guarantees.
/// </summary>
public partial class PhaseTwoWorkerTests
{
    /// <summary>
    /// Captures the <c>status</c> tag emitted with every
    /// <see cref="LatticeMetrics.ProviderRetryExhausted"/> measurement so
    /// a test can prove the phase-2 catch mapped a
    /// <see cref="RequestFailedException"/> to its numeric HTTP status
    /// bucket rather than the catch-all <c>unknown</c>.
    /// </summary>
    private sealed class RetryExhaustedStatusRecorder : IDisposable
    {
        private const string CounterName = "orleans.lattice.provider.retry.exhausted";
        private readonly MeterListener _listener;

        public List<string> Statuses { get; } = new();

        public RetryExhaustedStatusRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == CounterName)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagStatus)
                {
                    lock (Statuses)
                    {
                        Statuses.Add(tag.Value?.ToString() ?? "(null)");
                    }
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task DisposeAsync_faults_a_commit_still_parked_in_the_coalescing_window()
    {
        // With a positive coalescing window and a single arrival (below
        // the 49-batch ceiling), the drain loop parks in Task.Delay
        // BEFORE the commit loop, so the commit sits in _pending. Disposal
        // cancels that delay; the OperationCanceledException falls through
        // to the drain loop's finally, which faults every leftover pending
        // commit with ObjectDisposedException. The submit delegate is
        // never invoked on this path.
        var submitter = new RecordingSubmitter();
        var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.FromSeconds(60), commitTimeout: null);

        var parked = worker.EnqueueAsync(0L, 4L);

        // Let the drain loop read the arrival into _pending and enter the
        // coalescing Task.Delay before we dispose.
        await Task.Delay(250).ConfigureAwait(false);
        Assert.That(parked.IsCompleted, Is.False, "the commit must still be parked in the coalescing window");
        Assert.That(submitter.Calls.Count, Is.EqualTo(0), "submit must not run while parked in the window");

        await worker.DisposeAsync().ConfigureAwait(false);

        try
        {
            await parked.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            Assert.Fail("expected the parked commit to fault with ObjectDisposedException");
        }
        catch (ObjectDisposedException)
        {
            // expected: faulted by the drain-loop shutdown finally
        }
    }

    [Test]
    public async Task CommitBatchAsync_request_failed_faults_later_pending_commits_with_sdk_status()
    {
        // Prime one gated commit so the drain loop is parked awaiting its
        // submit. While gated, enqueue a large backlog that piles up in
        // the channel. Release the gate: the priming commit succeeds, the
        // post-commit drain folds the whole backlog into _pending, the
        // worker coalesces the first 49 into one transaction, and that
        // submit throws a RequestFailedException. The catch must fault the
        // 49-strong group AND every later still-pending commit, and stamp
        // the retry-exhausted counter with the SDK's numeric status.
        using var recorder = new RetryExhaustedStatusRecorder();
        var primed = 0;
        var rfe = new RequestFailedException(503, "Server busy", "ServerBusy", innerException: null);
        var gate = new TaskCompletionSource();
        var submitter = new RecordingSubmitter((_, _) =>
        {
            var n = Interlocked.Increment(ref primed);
            return n switch
            {
                1 => gate.Task,
                2 => Task.FromException(rfe),
                _ => Task.CompletedTask,
            };
        });
        await using var worker = new PhaseTwoWorker(submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero);

        var priming = worker.EnqueueAsync(0L, 0L);
        await WaitForCallsAsync(submitter, 1).ConfigureAwait(false);

        // 74 more commits (offsets 1..74). After the gate releases these
        // fold into _pending; the first 49 (1..49) coalesce into the
        // faulting submit, leaving 25 (50..74) as later-pending commits.
        var backlog = new List<Task>(74);
        for (var i = 1; i <= 74; i++)
        {
            backlog.Add(worker.EnqueueAsync(i, i));
        }

        gate.SetResult();
        await priming.ConfigureAwait(false);

        // Offset 74 is guaranteed to be one of the later-pending commits
        // (the SortedSet drains the 49 smallest first), so it faults via
        // the _pending fan-out (not the coalesced group), pinning that
        // branch specifically.
        var later = backlog[^1];
        try
        {
            await later.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            Assert.Fail("expected a later-pending commit to fault with RequestFailedException");
        }
        catch (RequestFailedException observed)
        {
            Assert.That(observed.Status, Is.EqualTo(503));
        }

        // Every backlogged commit must have faulted (49 in the coalesced
        // group + 25 later-pending), and the retry-exhausted counter must
        // carry the numeric SDK status, not the "unknown" bucket.
        foreach (var task in backlog)
        {
            Assert.That(task.IsFaulted, Is.True, "every backlogged commit must fault all-or-nothing");
        }

        Assert.That(recorder.Statuses, Has.Some.EqualTo("503"),
            "the RequestFailedException arm of the status-tag resolver must emit the numeric HTTP status");
    }

    [Test]
    public async Task CommitTimeout_abandoned_submit_that_faults_is_observed()
    {
        // The abandoned-submit observer only runs its fault-swallowing
        // continuation when the abandoned task actually FAULTS. The
        // existing timeout tests cancel their submit task (TrySetCanceled),
        // which never triggers an OnlyOnFaulted continuation. Here the
        // submit task instead faults with an OperationCanceledException
        // (TrySetException), so awaiting it throws OCE, the deadline filter
        // matches, ObserveAbandonedSubmit attaches its continuation to a
        // genuinely faulted task, and that continuation observes the
        // exception. The commit still surfaces a TimeoutException outward.
        var submitter = new RecordingSubmitter((_, ct) =>
        {
            var tcs = new TaskCompletionSource();
            ct.Register(() => tcs.TrySetException(new OperationCanceledException(ct)));
            return tcs.Task;
        });
        await using var worker = new PhaseTwoWorker(
            submitter.SubmitAsync, ManifestPartitionKey, TimeSpan.Zero,
            commitTimeout: TimeSpan.FromMilliseconds(50));

        await AssertEventuallyFaultsWithTimeoutAsync(worker.EnqueueAsync(0L, 4L));
    }
}
