using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// The phase-2 commit fence (issue #5810): every fenced transaction writes
/// <c>TAIL</c> conditionally on the ETag the fence holds, never below the value it
/// observed, in the one transaction the commit already makes; a batch claimed under
/// an older generation is refused without a submit; and a failed commit invalidates
/// the fence.
/// </summary>
public partial class PhaseTwoWorkerTests
{
    /// <summary>
    /// Records every fenced submit and returns a fresh ETag for <c>TAIL</c>, or throws
    /// the configured failure.
    /// </summary>
    private sealed class FencedSubmitter
    {
        private int _count;

        public List<IReadOnlyList<TableTransactionAction>> Submits { get; } = new();

        public Exception? Failure { get; set; }

        public Task<ETag?> SubmitAsync(IReadOnlyList<TableTransactionAction> actions, CancellationToken cancellationToken)
        {
            lock (Submits)
            {
                Submits.Add(actions.ToArray());
            }

            if (Failure is { } failure)
            {
                return Task.FromException<ETag?>(failure);
            }

            return Task.FromResult<ETag?>(new ETag($"tail-{Interlocked.Increment(ref _count)}"));
        }
    }

    private static TableTransactionAction[] TailActions(IReadOnlyList<TableTransactionAction> actions) =>
        actions.Where(a => a.Entity.RowKey == AzureTableWalStorageProvider.TailRowKey).ToArray();

    [Test]
    public async Task A_fenced_worker_makes_one_transaction_per_commit_as_an_unfenced_one_does()
    {
        // Steady-path cost: the fence adds no transaction and no read. Each commit is
        // still exactly one submit, carrying exactly one TAIL write.
        const int commits = 8;
        var unfenced = new RecordingSubmitter();
        await using (var worker = new PhaseTwoWorker(unfenced.SubmitAsync, ManifestPartitionKey))
        {
            for (var i = 0; i < commits; i++)
            {
                await worker.EnqueueAsync(i, i);
            }
        }

        var fenced = new FencedSubmitter();
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var generation = fence.Fill(guard, tailETag: null, tail: -1L, writtenUpperBound: -1L);
        await using (var worker = new PhaseTwoWorker(fenced.SubmitAsync, ManifestPartitionKey, fence))
        {
            for (var i = 0; i < commits; i++)
            {
                await worker.EnqueueAsync(i, i, generation: generation);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(fenced.Submits, Has.Count.EqualTo(unfenced.Calls.Count), "the fence changed the number of transactions");
            Assert.That(fenced.Submits, Has.Count.EqualTo(commits));
            Assert.That(fenced.Submits.Select(s => TailActions(s).Length), Is.All.EqualTo(1), "every fenced commit writes TAIL once");
            Assert.That(fence.TryGetGeneration(out var after), Is.True, "a successful commit invalidated the fence");
            Assert.That(after, Is.EqualTo(generation));
        });
    }

    [Test]
    public async Task A_fenced_commit_conditions_tail_on_the_etag_the_previous_commit_left()
    {
        var submitter = new FencedSubmitter();
        var fence = new WalShardCommitFence();
        var generation = fence.Fill(new WalShardOverlapGuard(), tailETag: null, tail: -1L, writtenUpperBound: -1L);
        await using var worker = new PhaseTwoWorker(submitter.SubmitAsync, ManifestPartitionKey, fence);

        await worker.EnqueueAsync(0, 1, generation: generation);
        await worker.EnqueueAsync(2, 3, generation: generation);
        await worker.EnqueueAsync(4, 5, generation: generation);

        var tails = submitter.Submits.Select(s => TailActions(s).Single()).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(tails[0].ActionType, Is.EqualTo(TableTransactionActionType.Add), "an absent TAIL is created, never upserted");
            Assert.That(tails[1].ActionType, Is.EqualTo(TableTransactionActionType.UpdateReplace));
            Assert.That(tails[1].ETag, Is.EqualTo(new ETag("tail-1")), "the second commit is not conditioned on the first's ETag");
            Assert.That(tails[2].ETag, Is.EqualTo(new ETag("tail-2")));
            Assert.That(tails.Select(t => ((AzureTableWalEntity)t.Entity).Offset), Is.EqualTo(new[] { 1L, 3L, 5L }));
        });
    }

    [Test]
    public async Task A_fenced_commit_never_writes_tail_below_the_value_the_fence_observed()
    {
        // Out-of-order commits are legitimate under pipelining, so a commit below the
        // observed TAIL is not refused; it must simply never lower TAIL.
        var submitter = new FencedSubmitter();
        var fence = new WalShardCommitFence();
        var generation = fence.Fill(new WalShardOverlapGuard(), new ETag("observed"), tail: 9L, writtenUpperBound: 9L);
        await using var worker = new PhaseTwoWorker(submitter.SubmitAsync, ManifestPartitionKey, fence);

        await worker.EnqueueAsync(3, 4, generation: generation);

        var tail = TailActions(submitter.Submits.Single()).Single();
        Assert.Multiple(() =>
        {
            Assert.That(((AzureTableWalEntity)tail.Entity).Offset, Is.EqualTo(9L), "the commit lowered TAIL");
            Assert.That(tail.ETag, Is.EqualTo(new ETag("observed")));
        });
    }

    [Test]
    public async Task A_commit_claimed_under_an_older_generation_is_refused_without_a_submit()
    {
        var submitter = new FencedSubmitter();
        var guard = new WalShardOverlapGuard();
        var fence = new WalShardCommitFence();
        var stale = fence.Fill(guard, new ETag("first"), tail: 3L, writtenUpperBound: 3L);
        fence.Invalidate();
        fence.Fill(guard, new ETag("second"), tail: 7L, writtenUpperBound: 7L);
        await using var worker = new PhaseTwoWorker(submitter.SubmitAsync, ManifestPartitionKey, fence);

        Assert.ThrowsAsync<InvalidOperationException>(() => worker.EnqueueAsync(4, 5, generation: stale));
        Assert.That(submitter.Submits, Is.Empty, "a batch claimed under an older generation was submitted");
    }

    [Test]
    public async Task A_refused_tail_write_invalidates_the_fence_and_refuses_the_claims_made_under_it()
    {
        var submitter = new FencedSubmitter
        {
            Failure = new RequestFailedException(412, "The update condition specified in the request was not satisfied."),
        };
        var fence = new WalShardCommitFence();
        var generation = fence.Fill(new WalShardOverlapGuard(), new ETag("observed"), tail: 3L, writtenUpperBound: 3L);
        await using var worker = new PhaseTwoWorker(submitter.SubmitAsync, ManifestPartitionKey, fence);

        Assert.ThrowsAsync<RequestFailedException>(() => worker.EnqueueAsync(4, 5, generation: generation));
        Assert.That(fence.TryGetGeneration(out _), Is.False, "a refused TAIL write left the fence holding its ETag");

        submitter.Failure = null;
        Assert.ThrowsAsync<InvalidOperationException>(() => worker.EnqueueAsync(6, 7, generation: generation));
        Assert.That(submitter.Submits, Has.Count.EqualTo(1), "a claim made under the refused generation was submitted");
    }

    [Test]
    public async Task A_commit_whose_response_carries_no_etag_invalidates_the_fence()
    {
        var fence = new WalShardCommitFence();
        var generation = fence.Fill(new WalShardOverlapGuard(), tailETag: null, tail: -1L, writtenUpperBound: -1L);
        await using var worker = new PhaseTwoWorker(
            (_, _) => Task.FromResult<ETag?>(null), ManifestPartitionKey, fence);

        await worker.EnqueueAsync(0, 0, generation: generation);

        Assert.That(fence.TryGetGeneration(out _), Is.False, "the next commit could be conditioned on an ETag nobody observed");
    }
}
