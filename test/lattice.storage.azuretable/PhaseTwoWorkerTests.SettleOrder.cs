namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// #5843: a phase-2 commit releases its outstanding-commit count before it reports the
/// commit complete, exactly once. The completion runs its continuations asynchronously, so a
/// caller can resume the instant it is set; releasing the count afterwards let that caller see
/// the tracker still busy with its own commit. Each detector holds the tracker's gate, which
/// every release takes, on a thread of its own while the submit returns: a settle that releases
/// first cannot complete the commit while the gate is held, whereas one that completes first
/// does so at once - no timing race decides the verdict.
/// </summary>
public partial class PhaseTwoWorkerTests
{
    private static async Task<(Task Commit, bool CompletedWhileHeld)> SettleWhileTrackerGateIsHeldAsync(bool fault)
    {
        var submitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var submitter = new RecordingSubmitter(async (_, _) =>
        {
            await submitted.Task.ConfigureAwait(false);
            if (fault)
            {
                throw new InvalidOperationException("injected: the phase-2 transaction failed");
            }
        });
        var worker = NewWorker(submitter);
        try
        {
            var commit = worker.EnqueueAsync(0L, 4L);
            await WaitForCallsAsync(submitter, 1);

            using var held = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var holder = new Thread(() =>
            {
                lock (worker.OutstandingCommits.GateForTesting)
                {
                    held.Set();
                    release.Wait();
                }
            })
            { IsBackground = true };
            holder.Start();
            held.Wait();

            bool completedWhileHeld;
            try
            {
                submitted.SetResult();
                completedWhileHeld = await Task.WhenAny(commit, Task.Delay(TimeSpan.FromSeconds(2))).ConfigureAwait(false) == commit;
            }
            finally
            {
                release.Set();
                holder.Join();
            }

            try
            {
                await commit.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            }
            catch (InvalidOperationException) when (fault)
            {
            }

            Assert.That(worker.OutstandingCommits.Active, Is.Zero, "the tracker was not idle once the commit was reported");
            return (commit, completedWhileHeld);
        }
        finally
        {
            await worker.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Test]
    public async Task A_committed_batch_releases_its_outstanding_count_before_it_is_reported_complete()
    {
        var (commit, completedWhileHeld) = await SettleWhileTrackerGateIsHeldAsync(fault: false);

        Assert.Multiple(() =>
        {
            Assert.That(completedWhileHeld, Is.False, "the commit was reported complete before its outstanding count was released");
            Assert.That(commit.IsCompletedSuccessfully, Is.True);
        });
    }

    [Test]
    public async Task A_faulted_batch_releases_its_outstanding_count_before_it_is_reported_faulted()
    {
        var (commit, completedWhileHeld) = await SettleWhileTrackerGateIsHeldAsync(fault: true);

        Assert.Multiple(() =>
        {
            Assert.That(completedWhileHeld, Is.False, "the commit was reported faulted before its outstanding count was released");
            Assert.That(commit.IsFaulted, Is.True);
        });
    }

    [Test]
    public void A_phase_two_completion_is_settled_at_most_once()
    {
        var completion = new PhaseTwoWorker.PhaseTwoCompletion();

        Assert.Multiple(() =>
        {
            Assert.That(completion.TryClaimSettle(), Is.True);
            Assert.That(completion.TryClaimSettle(), Is.False, "a second settle would release the outstanding count twice");
        });
    }
}
