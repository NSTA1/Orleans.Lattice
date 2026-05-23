using System.Threading.Channels;
using Azure;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-shard phase-two commit worker for the two-phase append protocol
/// (the strict offset-ordered phase-2 stage of the two-phase WAL
/// commit). Phase 1 of an append commits the entry
/// rows + per-batch HEAD row atomically inside a distinct batch
/// partition - that step gets true cross-batch parallelism because
/// each batch hits its own Azure Tables partition server. Phase 2
/// commits one manifest row (<c>M{startOffset:D19}</c>) plus the
/// shard's tail-pointer upsert (<c>TAIL</c>) atomically inside the
/// shard's manifest partition; the manifest tail must be monotonic so
/// the worker drains pending phase-2 commits in strict
/// <c>startOffset</c> order regardless of phase-1 completion order.
/// <para>
/// The worker is lazily created per <c>(treeId, shardIndex)</c> the
/// first time a phase-2 commit is enqueued and lives for the lifetime
/// of the owning <see cref="AzureTableWalStorageProvider"/>. One Task
/// + one bounded <see cref="Channel{T}"/> per active shard; shards
/// are bounded by Orleans activation counts, so the overhead
/// amortises across the lifetime of the silo.
/// </para>
/// <para>
/// <b>Ordering primitive.</b> The worker uses a <see cref="SortedSet{T}"/>
/// keyed by <c>startOffset</c> as a min-heap: arriving phase-2 work
/// is enqueued under the channel's monitor, the worker drains the
/// channel into the sorted set, then commits in ascending offset
/// order. A pending commit with offset <c>K</c> only fires when every
/// pending offset less than <c>K</c> has already fired; this is the
/// "manifest-consistent-through" watermark the reconciliation step
/// (stage 2c) relies on.
/// </para>
/// <para>
/// <b>Failure model.</b> Mirrors <c>WalShardGrain</c>'s sticky-failure
/// behaviour. A failed manifest transaction faults the in-flight work
/// item plus every later pending work item, because their tail offsets
/// are now stale and committing them would advance TAIL past a hole
/// the reconciliation step needs to see. The worker then continues
/// to drain new arrivals (which will start at the post-failure
/// resync's recovered offset and are independent of the faulted
/// window).
/// </para>
/// </summary>
internal sealed class PhaseTwoWorker : IAsyncDisposable
{
    private readonly Func<IReadOnlyList<TableTransactionAction>, CancellationToken, Task> _submit;
    private readonly string _manifestPartitionKey;
    private readonly Channel<PhaseTwoCommit> _arrivals;
    private readonly Task _drainLoop;
    private readonly CancellationTokenSource _shutdown;
    private readonly SortedSet<PhaseTwoCommit> _pending;
    private long _highestCommittedEndOffset = -1L;

    /// <summary>
    /// Production constructor. Captures the provider's table-client
    /// lookup and adapts it to the worker's narrower transaction-submit
    /// seam so the worker has no <see cref="TableClient"/> dependency
    /// of its own (which keeps the test surface lean).
    /// </summary>
    public PhaseTwoWorker(
        Func<CancellationToken, ValueTask<TableClient>> tableProvider,
        string manifestPartitionKey)
        : this(
            async (actions, cancellationToken) =>
            {
                var table = await tableProvider(cancellationToken).ConfigureAwait(false);
                await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
            },
            manifestPartitionKey)
    {
    }

    /// <summary>
    /// Test-only constructor that lets a unit test substitute the
    /// transaction-submit seam with a recording / failing delegate.
    /// The submit delegate receives the exact
    /// <see cref="TableTransactionAction"/> sequence the worker would
    /// have sent to Azure Tables; its returned <see cref="Task"/>
    /// stand-in determines whether the worker treats the commit as
    /// durable (completed) or faulted (faulted).
    /// </summary>
    internal PhaseTwoWorker(
        Func<IReadOnlyList<TableTransactionAction>, CancellationToken, Task> submit,
        string manifestPartitionKey)
    {
        _submit = submit;
        _manifestPartitionKey = manifestPartitionKey;
        _arrivals = Channel.CreateUnbounded<PhaseTwoCommit>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _pending = new SortedSet<PhaseTwoCommit>(PhaseTwoCommitByStartOffset.Instance);
        _shutdown = new CancellationTokenSource();

        // Suppress ExecutionContext flow into the drain loop so the
        // background pump does not inherit AsyncLocal state (e.g.
        // Activity.Current, AmbientTransaction) from the first
        // caller that activated this worker. The drain loop services
        // every subsequent append on the shard, so any flowed value
        // would silently leak across logically independent commits;
        // suppressing the flow is the standard pattern for a
        // long-lived background pump.
        using (ExecutionContext.SuppressFlow())
        {
            _drainLoop = Task.Run(() => DrainLoopAsync(_shutdown.Token));
        }
    }

    /// <summary>
    /// Enqueues a phase-2 commit and returns a task that completes
    /// when the worker has durably written the manifest row + TAIL
    /// pointer. The task faults with the underlying
    /// <see cref="Exception"/> on transaction failure.
    /// </summary>
    public Task EnqueueAsync(long startOffset, long endOffsetInclusive, bool hasCandidateRow = true)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var commit = new PhaseTwoCommit(startOffset, endOffsetInclusive, hasCandidateRow, tcs);
        if (!_arrivals.Writer.TryWrite(commit))
        {
            // Unbounded channel - this is only reachable if the
            // channel was already completed by Dispose.
            tcs.TrySetException(new ObjectDisposedException(nameof(PhaseTwoWorker)));
        }
        return tcs.Task;
    }

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        _shutdown.Cancel();
        _arrivals.Writer.TryComplete();
        try
        {
            await _drainLoop.ConfigureAwait(false);
        }
        catch
        {
            // Shutdown swallows individual failures - they are already
            // surfaced to the per-commit TCSs.
        }
        _shutdown.Dispose();
    }

    /// <summary>
    /// Maximum number of pending phase-2 commits coalesced into a
    /// single Azure Tables transaction. The transaction action cap is
    /// 100; each committed batch contributes two actions (one
    /// <c>C{startOffset:D19}</c> delete plus one
    /// <c>M{startOffset:D19}</c> add) and one action is reserved for
    /// the shared <c>TAIL</c> upsert, so the per-transaction ceiling
    /// is <c>(100 - 1) / 2 = 49</c> coalesced batches. Under burst
    /// load (e.g. <c>WalMaxPendingBatches</c> raised, many phase-1
    /// transactions completing concurrently) this reduces phase-2
    /// round-trip count by up to 49x without weakening the strict
    /// offset-FIFO invariant - commits are still drained in ascending
    /// start-offset order and the coalesced transaction is itself
    /// atomic.
    /// </summary>
    private const int MaxBatchedManifestRows = 49;

    private readonly List<PhaseTwoCommit> _batchBuffer = new(MaxBatchedManifestRows);

    private async Task DrainLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _arrivals.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                // Drain everything currently in the channel into the
                // sorted set before committing any of it; this gives
                // the worker a complete-as-of-now view of pending
                // offsets, so a burst of phase-1 completions in
                // reverse-offset order still commits in ascending
                // order.
                while (_arrivals.Reader.TryRead(out var arriving))
                {
                    _pending.Add(arriving);
                }

                while (_pending.Count > 0)
                {
                    // Coalesce up to MaxBatchedManifestRows pending
                    // commits into one phase-2 transaction. The
                    // sorted-set min-removal preserves strict
                    // offset-FIFO across the coalesced group; TAIL is
                    // upserted to the highest endOffsetInclusive in
                    // the group because the group is contiguous in
                    // commit order by the strict-ordering invariant.
                    _batchBuffer.Clear();
                    while (_batchBuffer.Count < MaxBatchedManifestRows && _pending.Count > 0)
                    {
                        var next = _pending.Min!;
                        _pending.Remove(next);
                        _batchBuffer.Add(next);
                    }

                    await CommitBatchAsync(_batchBuffer, cancellationToken).ConfigureAwait(false);

                    // After a commit, drain any new arrivals that
                    // landed during the await before picking the
                    // next minimum; without this a newly-arrived
                    // smaller-offset commit could be skipped while
                    // a larger-offset already-pending commit fires.
                    while (_arrivals.Reader.TryRead(out var late))
                    {
                        _pending.Add(late);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        finally
        {
            // Fault any commits still pending at shutdown.
            foreach (var leftover in _pending)
            {
                leftover.Completion.TrySetException(
                    new ObjectDisposedException(nameof(PhaseTwoWorker)));
            }
            _pending.Clear();
        }
    }

    private async Task CommitBatchAsync(List<PhaseTwoCommit> commits, CancellationToken cancellationToken)
    {
        // commits is non-empty and sorted ascending by StartOffset by
        // construction (SortedSet.Min drains in ascending order).
        var highestEndOffset = commits[^1].EndOffsetInclusive;
        try
        {
            var actions = new List<TableTransactionAction>((commits.Count * 2) + 1);
            for (var i = 0; i < commits.Count; i++)
            {
                if (commits[i].HasCandidateRow)
                {
                    // Delete the candidate-row stamped by phase 0. Atomic
                    // with the M-row insert below: either both land (the
                    // committed-batch invariant holds) or neither does
                    // (the C-row remains, reconciliation re-discovers the
                    // batch as an orphan). The C-row's ETag is unknown
                    // here because the worker did not write it; "*" is
                    // the documented sentinel for an unconditional
                    // delete inside a transaction. Skipped entirely when
                    // the originating AppendBatchAsync ran with
                    // EliminateCandidateRowOnHotPath = true - there is
                    // no C-row to delete in that mode and including the
                    // delete would fail the whole transaction with 404.
                    actions.Add(new TableTransactionAction(
                        TableTransactionActionType.Delete,
                        new AzureTableWalEntity
                        {
                            PartitionKey = _manifestPartitionKey,
                            RowKey = AzureTableWalStorageProvider.BuildCandidateRowKey(commits[i].StartOffset),
                            Offset = commits[i].EndOffsetInclusive,
                            Payload = null,
                        },
                        ETag.All));
                }

                actions.Add(new TableTransactionAction(
                    TableTransactionActionType.Add,
                    new AzureTableWalEntity
                    {
                        PartitionKey = _manifestPartitionKey,
                        RowKey = AzureTableWalStorageProvider.BuildManifestRowKey(commits[i].StartOffset),
                        Offset = commits[i].EndOffsetInclusive,
                        Payload = null,
                    }));
            }

            // TAIL is upserted to the highest endOffsetInclusive
            // observed across every commit this worker has durably
            // landed, not just the current coalesced group. Two
            // concurrent <c>AppendBatchAsync</c> calls into the same
            // shard whose phase-0/1 races complete out of
            // start-offset order arrive at the worker in the
            // "wrong" order; the sorted set restores ascending
            // start-offset order *across the pending set*, but the
            // worker can still commit batches as separate phase-2
            // transactions (e.g. when a higher-offset batch arrives
            // while a lower-offset batch is mid-submit, the higher
            // one ends up in its own later commit). If we upserted
            // the current group's max blindly, a later
            // smaller-offset commit would *regress* TAIL by
            // overwriting the higher value already on disk. We
            // instead clamp the persisted TAIL at the high-water
            // mark across this worker's lifetime; the upsert is
            // skipped entirely (no action in the transaction) when
            // the current group's max does not advance the mark.
            var tailToPersist = Math.Max(highestEndOffset, _highestCommittedEndOffset);
            var tailAdvances = tailToPersist > _highestCommittedEndOffset
                || (_highestCommittedEndOffset == -1L && highestEndOffset >= 0L);
            if (tailAdvances)
            {
                actions.Add(new TableTransactionAction(
                    TableTransactionActionType.UpsertReplace,
                    new AzureTableWalEntity
                    {
                        PartitionKey = _manifestPartitionKey,
                        RowKey = AzureTableWalStorageProvider.TailRowKey,
                        Offset = tailToPersist,
                        Payload = null,
                    }));
            }

            await _submit(actions, cancellationToken).ConfigureAwait(false);

            _highestCommittedEndOffset = tailToPersist;
            for (var i = 0; i < commits.Count; i++)
            {
                commits[i].Completion.TrySetResult();
            }
        }
        catch (Exception ex)
        {
            // Azure Tables transactions are all-or-nothing, so a
            // failure faults every commit in the coalesced group.
            // Also fault every later still-pending commit - their
            // tail offsets are now stale relative to the recovered
            // TAIL and committing them would silently advance the
            // pointer past a hole the reconciliation step needs to
            // see.
            for (var i = 0; i < commits.Count; i++)
            {
                commits[i].Completion.TrySetException(ex);
            }
            foreach (var pending in _pending)
            {
                pending.Completion.TrySetException(ex);
            }
            _pending.Clear();
        }
    }

    /// <summary>
    /// Phase-2 commit work item. Carries the offsets the manifest row
    /// + TAIL pointer encode, plus the caller's <see cref="TaskCompletionSource"/>
    /// the worker signals on durable commit. Equality is by start
    /// offset alone so the sorted set treats two commits sharing a
    /// start offset as duplicates (which would be a phase-1
    /// invariant violation upstream; defensive only).
    /// </summary>
    internal readonly record struct PhaseTwoCommit(
        long StartOffset,
        long EndOffsetInclusive,
        bool HasCandidateRow,
        TaskCompletionSource Completion);

    /// <summary>
    /// <see cref="IComparer{T}"/> that orders
    /// <see cref="PhaseTwoCommit"/> values by <c>StartOffset</c>; lets
    /// the <see cref="SortedSet{T}"/> act as a min-heap keyed on the
    /// strict offset-FIFO invariant.
    /// </summary>
    private sealed class PhaseTwoCommitByStartOffset : IComparer<PhaseTwoCommit>
    {
        public static readonly PhaseTwoCommitByStartOffset Instance = new();

        public int Compare(PhaseTwoCommit x, PhaseTwoCommit y) =>
            x.StartOffset.CompareTo(y.StartOffset);
    }
}
