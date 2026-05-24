using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Durable Azure Table Storage <see cref="IWalStorageProvider"/>. Uses
/// a two-phase per-batch / manifest schema for true
/// cross-batch partition-server parallelism.
/// <para>
/// <b>Schema.</b> Every <see cref="AppendBatchAsync"/> call lands in
/// its own batch partition keyed
/// <c>_b_|{treeId}|{shardIndex}|S{startOffset:D19}</c>; one row per
/// entry, row-key <c>E{offset:D19}</c>. Each shard also owns one
/// manifest partition keyed <c>_m_|{treeId}|{shardIndex}</c>; it holds
/// one row per committed batch (row-key <c>M{startOffset:D19}</c>,
/// Offset column = <c>endOffsetInclusive</c>) plus a single
/// <c>TAIL</c> row whose Offset column is the monotonic tail of the
/// shard. <see cref="GetHighestOffsetAsync"/> point-reads the TAIL
/// row; <see cref="ReadAsync"/> ascending-scans the manifest partition
/// for overlapping batches and streams their entries.
/// </para>
/// <para>
/// <b>Atomicity (phase 1).</b> Every entry-row write for a batch is
/// committed in a single
/// <see cref="TableClient.SubmitTransactionAsync"/> against the batch
/// partition, so a batch is either fully durable or invisible. The
/// transaction holds up to <see cref="MaxEntriesPerBatch"/> add
/// actions; there is no per-batch HEAD sentinel because activation
/// reconciliation (stage 2c) derives <c>endOffsetInclusive</c> from a
/// <c>Top(1) DESC</c> query over the batch partition's entry rows.
/// </para>
/// <para>
/// <b>Atomicity (phase 2).</b> After phase 1 commits, the provider
/// hands off to a per-shard <see cref="PhaseTwoWorker"/>. The worker
/// drains pending commits in strict <c>startOffset</c> order and
/// coalesces up to 99 manifest-row adds plus one <c>TAIL</c> upsert
/// into a single phase-2 transaction. The strict-offset drain order
/// makes TAIL unconditionally monotonic regardless of phase-1
/// completion order, and the coalescing collapses N round-trips into
/// one under burst load. By default <see cref="AppendBatchAsync"/>
/// awaits the phase-2 completion so post-append
/// <see cref="GetHighestOffsetAsync"/> observes the new TAIL. When
/// <see cref="AzureTableWalStorageOptions.PipelinePhaseTwoCommits"/>
/// is <see langword="true"/>, <see cref="AppendBatchAsync"/> instead
/// awaits the <i>previous</i> append's phase-2 task on the same
/// shard before returning - phase 2 of batch N runs in parallel with
/// phase 0+1 of batch N+1, halving the steady-state request-path
/// latency under <c>WalMaxPendingBatches = 1</c> while preserving
/// every durability and ordering invariant the synchronous mode
/// enforces (sticky failure still surfaces, just on the next call;
/// reconciliation still rolls forward any phase-1-durable batch
/// after a crash).
/// </para>
/// <para>
/// <b>Capacity.</b> Azure Tables caps a single transaction at 100
/// actions and 4&#160;MiB. Phase 1 holds entry rows only (no HEAD
/// sentinel) and accepts up to <see cref="MaxEntriesPerBatch"/>
/// entries. Phase 2 holds up to 99 manifest-row adds plus the
/// shared TAIL upsert. Callers chunk larger logical batches
/// upstream; the WAL grain's <c>LatticeOptions.WalMaxPendingBatches</c>
/// already keeps batches well below this cap in the canonical
/// replication path.
/// </para>
/// <para>
/// <b>Thread safety.</b> Instances are safe for concurrent calls
/// across distinct partitions. Concurrent calls into the same shard
/// land in distinct batch partitions during phase 1 (no contention)
/// and serialise through the per-shard phase-2 worker for phase 2.
/// With the default <c>WalMaxPendingBatches = 1</c>, only one
/// <see cref="AppendBatchAsync"/> is in-flight per shard. With
/// <c>&gt; 1</c>, a phase-1 failure on an earlier offset while a later
/// offset has already enqueued phase 2 will be caught by activation
/// reconciliation (stage 2c) and surfaced on next activation.
/// </para>
/// </summary>
public sealed partial class AzureTableWalStorageProvider : IWalStorageProvider, IAsyncDisposable
{
    /// <summary>
    /// Maximum number of <see cref="WalEntry"/> values that can be
    /// appended in a single <see cref="AppendBatchAsync"/> call. With
    /// the two-phase per-batch schema, phase 1 holds
    /// only entry rows - no per-batch HEAD sentinel - so the full
    /// 100-action Azure Tables transaction cap is available for
    /// entries.
    /// </summary>
    public const int MaxEntriesPerBatch = 100;

    /// <summary>
    /// Row-key prefix for entry rows. Sorts before <see cref="HeadRowKey"/>
    /// so a <see cref="TableClient.QueryAsync{T}"/> filtering on
    /// <c>RowKey ge 'E' and RowKey lt 'F'</c> returns only entries.
    /// </summary>
    internal const string EntryRowKeyPrefix = "E";

    /// <summary>
    /// Row-key for the per-partition head-pointer sentinel. Sorts after
    /// every entry row (because <c>'H' &gt; 'E'</c>) so the entry-range
    /// query can use a tight upper bound.
    /// </summary>
    internal const string HeadRowKey = "HEAD";

    /// <summary>
    /// Per-batch partition prefix introduced by the per-batch partition
    /// + manifest schema. Every <see cref="AppendBatchAsync"/>
    /// call lands in its own partition keyed as
    /// <c>{BatchPartitionPrefix}|{treeId}|{shardIndex}|S{startOffset:D19}</c>,
    /// giving concurrent appends true partition-server parallelism on
    /// the Azure Tables side. The leading marker is the minimal three
    /// bytes (<c>_b_</c>) so the partition key stays compact on every
    /// row (each row carries a copy on the wire); a longer marker like
    /// <c>__batch__</c> would add ~6 bytes per row across an entire
    /// shard's storage and network surface. The marker also makes the
    /// namespace disjoint from the manifest namespace
    /// (<see cref="ManifestPartitionPrefix"/>).
    /// </summary>
    internal const string BatchPartitionPrefix = "_b_";

    /// <summary>
    /// Per-shard manifest partition prefix introduced by the per-batch
    /// partition + manifest schema. Each shard has
    /// exactly one manifest partition keyed as
    /// <c>{ManifestPartitionPrefix}|{treeId}|{shardIndex}</c>, holding
    /// one row per committed batch plus the
    /// <see cref="TailRowKey"/> pointer that
    /// <see cref="GetHighestOffsetAsync"/> point-reads. Same length /
    /// disjointness rationale as <see cref="BatchPartitionPrefix"/>.
    /// </summary>
    internal const string ManifestPartitionPrefix = "_m_";

    /// <summary>
    /// Per-batch row-key prefix for manifest rows. Each committed batch
    /// contributes one row with key
    /// <c>{ManifestRowKeyPrefix}{startOffset:D19}</c> in the shard's
    /// manifest partition; the row's <c>Offset</c> column carries
    /// <c>endOffsetInclusive</c>. The D19 width (long.MaxValue is 19
    /// digits) makes the row keys lexicographically equivalent to the
    /// numeric start-offset order, so a <c>RowKey</c> range query
    /// against the manifest partition returns the batches that overlap
    /// a requested window in ascending offset order.
    /// </summary>
    internal const string ManifestRowKeyPrefix = "M";

    /// <summary>
    /// Per-batch row-key prefix for the candidate-index row written
    /// during phase 0 of an append. The row sits in the shard's
    /// manifest partition (one C-row per in-flight batch, key
    /// <c>{CandidateRowKeyPrefix}{startOffset:D19}</c>, payload = the
    /// batch's <c>endOffsetInclusive</c>) so reconciliation can
    /// discover orphans with a single anchored
    /// <c>RowKey ge 'C' and RowKey lt 'D'</c> query against the
    /// manifest partition - no cross-partition scan over the shard's
    /// live batch partitions. Phase 2 deletes the C-row atomically
    /// with the M-row insert and TAIL upsert, so once a batch is
    /// committed no C-row remains; a non-empty C-row scan therefore
    /// returns exactly the set of phase-1-without-phase-2 orphans.
    /// The leading character <c>'C'</c> sorts before
    /// <see cref="ManifestRowKeyPrefix"/> (<c>'M'</c>) and
    /// <see cref="TailRowKey"/> (<c>'T'</c>) so every existing manifest
    /// range query (<c>RowKey ge 'M' and RowKey lt 'T'</c>) excludes
    /// C-rows without modification.
    /// </summary>
    internal const string CandidateRowKeyPrefix = "C";

    /// <summary>
    /// Exclusive upper bound matching <see cref="CandidateRowKeyPrefix"/>.
    /// Used by reconciliation's anchored range query against the
    /// manifest partition: <c>RowKey ge 'C' and RowKey lt 'D'</c>
    /// returns the shard's outstanding C-rows in ascending
    /// start-offset order.
    /// </summary>
    internal const string CandidateRowKeyExclusiveUpperBound = "D";

    /// <summary>
    /// Row-key for the shard's tail pointer. Stored in the manifest
    /// partition; its <c>Offset</c> column holds the maximum committed
    /// <c>endOffsetInclusive</c> across every batch in the shard. The
    /// row sorts after every manifest entry row (because <c>'T' &gt;
    /// 'M'</c>), matching the
    /// <c>'HEAD' &gt; 'E'</c> convention from the per-batch schema and
    /// letting a manifest-range query use a tight upper bound that
    /// excludes the tail row.
    /// </summary>
    internal const string TailRowKey = "TAIL";

    private const int MaxTransactionActions = 100;

    /// <summary>
    /// Process-wide cache of percent-encoded tree-id segments. The
    /// percent-encoding of a tree id is a pure function of the input
    /// string and the same id is hashed on every <see cref="AppendBatchAsync"/>,
    /// <see cref="ReadAsync"/>, <see cref="TrimAsync"/>,
    /// <see cref="GetHighestOffsetAsync"/>, and
    /// <see cref="GetLowestOffsetAsync"/> call into the provider - i.e.
    /// on every WAL operation. Caching the encoded form here saves a
    /// per-call <c>Encoding.UTF8.GetBytes</c> byte-array and a
    /// <c>StringBuilder</c> chararray allocation on each
    /// <see cref="BuildBatchPartitionKey"/> / <see cref="BuildManifestPartitionKey"/>
    /// / <see cref="BuildPartitionKey"/> invocation. The cache is
    /// process-static (no per-instance state) because the encoded form
    /// is invariant; the dictionary is bounded by the active-tree
    /// set, which is naturally O(tens) in the canonical deployment.
    /// </summary>
    private static readonly ConcurrentDictionary<string, string> EncodedPartitionSegmentCache =
        new(StringComparer.Ordinal);

    private readonly AzureTableWalStorageOptions _options;
    private readonly Serializer<WalRecord> _serializer;

    /// <summary>
    /// Cached <see cref="LatticeMetrics.TagPipelinePhaseTwo"/> tag
    /// bound to this provider instance's effective
    /// <see cref="AzureTableWalStorageOptions.PipelinePhaseTwoCommits"/>
    /// setting. The provider is a singleton in DI so the value is
    /// fixed for the lifetime of the host; caching at construction
    /// time keeps the Phase A provider hot path allocation-free. The
    /// tag is emitted on <see cref="LatticeMetrics.ProviderCommitDuration"/>
    /// and <see cref="LatticeMetrics.ProviderRetryExhausted"/> so the
    /// attribution sweep can pivot between synchronous (default) and
    /// pipelined phase-2 modes in a single dashboard query.
    /// </summary>
    private readonly KeyValuePair<string, object?> _pipelinePhaseTwoTag;

    // Per-shard phase-2 workers, lazily created on first append for a
    // given (treeId, shardIndex). Each worker owns a single Task plus
    // a bounded SortedSet drain buffer; shards are bounded by Orleans
    // activation counts so the steady-state overhead is bounded by
    // the silo's active-shard set. Exposed as `internal` so the test
    // assembly can structurally pin the "one worker per (treeId,
    // shardIndex)" parallelism characteristic without reflection.
    internal readonly ConcurrentDictionary<string, PhaseTwoWorker> _phaseTwoWorkers =
        new(StringComparer.Ordinal);

    // Per-shard "previous batch's phase-2 task" slot, populated only
    // when AzureTableWalStorageOptions.PipelinePhaseTwoCommits is on.
    // AppendBatchAsync swaps the new batch's phase-2 task into the
    // slot atomically and awaits whatever was there before, so phase
    // 2 of batch N overlaps phase 0+1 of batch N+1 while still
    // surfacing N's failure on call N+1. Keyed by manifest partition
    // key (same key the worker dictionary uses) so the two
    // dictionaries align entry-for-entry without an extra lookup.
    // Internal so the test assembly can introspect the slot's
    // identity-based swap behaviour without reflection.
    internal readonly ConcurrentDictionary<string, Task> _pipelinedPhaseTwoTasks =
        new(StringComparer.Ordinal);

    private TableClient? _tableClient;
    private int _tableInitialised;
    private int _disposed;

    private readonly SemaphoreSlim _initLock = new(1, 1);

    /// <summary>
    /// Initialises the provider with the supplied options and Orleans
    /// serializer. Resolved from DI in the standard registration path
    /// (<see cref="LatticeAzureTableServiceCollectionExtensions.AddAzureTableWalStorage}/>);
    /// tests construct it directly with a serializer pulled from
    /// <c>new ServiceCollection().AddSerializer().BuildServiceProvider()</c>.
    /// </summary>
    public AzureTableWalStorageProvider(
        IOptions<AzureTableWalStorageOptions> options,
        Serializer<WalRecord> serializer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);
        _options = options.Value ?? throw new ArgumentException(
            $"{nameof(IOptions<AzureTableWalStorageOptions>)}.{nameof(IOptions<AzureTableWalStorageOptions>.Value)} returned null.",
            nameof(options));
        _serializer = serializer;
        _pipelinePhaseTwoTag = new KeyValuePair<string, object?>(
            LatticeMetrics.TagPipelinePhaseTwo,
            _options.PipelinePhaseTwoCommits);
    }

    /// <inheritdoc />
    public async Task AppendBatchAsync(
        string treeId,
        int shardIndex,
        IReadOnlyList<WalEntry> entries,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        if (entries.Count == 0)
        {
            return;
        }

        if (entries.Count > MaxEntriesPerBatch)
        {
            throw new ArgumentException(
                $"Azure Table Storage caps a single transactional batch at {MaxTransactionActions} actions; "
                + $"the supplied batch of {entries.Count} entries exceeds the per-call limit of {MaxEntriesPerBatch}. Chunk the batch upstream before calling.",
                nameof(entries));
        }

        // Validate the supplied offsets are dense ahead of any I/O so a
        // misuse fails before consuming a transaction. The leading
        // offset must be non-negative; subsequent offsets must equal
        // entries[0].Offset + i. Validation runs ahead of mutation so a
        // rejected batch leaves observable state untouched.
        var firstOffset = entries[0].Offset;
        if (firstOffset < 0L)
        {
            throw new ArgumentException(
                $"Append batch for '{treeId}/{shardIndex}' starts at a negative offset ({firstOffset}); WAL offsets are non-negative dense integers.",
                nameof(entries));
        }

        for (var i = 1; i < entries.Count; i++)
        {
            var expected = firstOffset + i;
            if (entries[i].Offset != expected)
            {
                throw new ArgumentException(
                    $"Append batch for '{treeId}/{shardIndex}' is not dense: entry {i} has offset {entries[i].Offset} but expected {expected}. "
                    + "Supplied offsets must equal entries[0].Offset + i for every i.",
                    nameof(entries));
            }
        }

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var endOffsetInclusive = firstOffset + entries.Count - 1;
        var eliminateCandidateRow = _options.EliminateCandidateRowOnHotPath;

        // Phase 0 (parallel with phase 1): stamp a candidate-row in
        // the shard's manifest partition so reconciliation can
        // discover this batch with a single anchored RowKey range
        // query if the silo crashes before phase 2 runs. Phase 2
        // deletes the C-row atomically with its M-row insert, so the
        // C-row's presence post-restart is the orphan signal.
        //
        // When EliminateCandidateRowOnHotPath is on, the C-row write
        // is skipped entirely and activation-time reconciliation
        // falls back to a cross-partition discovery scan that
        // enumerates batch partitions above TAIL. See
        // AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath
        // for the soundness argument.
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        var candidateTask = eliminateCandidateRow
            ? Task.CompletedTask
            : WriteCandidateRowAsync(
                table, manifestPartitionKey, firstOffset, endOffsetInclusive, cancellationToken);

        // Phase 1: write the entry rows into the batch's own partition
        // in a single transaction. Each batch hits a distinct Azure
        // Tables partition server so concurrent batches against the
        // same shard get true parallelism.
        var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, firstOffset);
        var phaseOneActions = new List<TableTransactionAction>(entries.Count);
        EncodeEntriesForBatch(batchPartitionKey, entries, phaseOneActions);
        var phaseOneTask = SubmitPhaseOneAsync(table, phaseOneActions, treeId, shardIndex, cancellationToken);

        // Await both before enqueueing phase 2 so a failure in either
        // surfaces synchronously to the caller and the worker never
        // sees a phase-2 commit whose phase 1 or phase 0 didn't land.
        await candidateTask.ConfigureAwait(false);
        await phaseOneTask.ConfigureAwait(false);

        // Phase 2: hand the (startOffset, endOffsetInclusive) pair to
        // the per-shard worker. The worker batches up to 49 phase-2
        // commits (each contributing 1 C-delete + 1 M-insert action,
        // plus the shared TAIL upsert, fitting under the 100-action
        // Azure Tables transaction cap) into one manifest-partition
        // transaction in strict ascending start-offset order, then
        // upserts TAIL to the group's highest endOffsetInclusive.
        await DispatchPhaseTwoAsync(manifestPartitionKey, treeId, shardIndex, firstOffset, endOffsetInclusive, hasCandidateRow: !eliminateCandidateRow, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        if (encodedEntries.Length != offsets.Length)
        {
            throw new ArgumentException(
                $"Encoded segment count ({encodedEntries.Length}) does not match offset count ({offsets.Length}); the two sequences must be parallel.",
                nameof(encodedEntries));
        }

        if (encodedEntries.Length == 0)
        {
            return;
        }

        if (encodedEntries.Length > MaxEntriesPerBatch)
        {
            throw new ArgumentException(
                $"Azure Table Storage caps a single transactional batch at {MaxTransactionActions} actions; "
                + $"the supplied batch of {encodedEntries.Length} entries exceeds the per-call limit of {MaxEntriesPerBatch}. Chunk the batch upstream before calling.",
                nameof(encodedEntries));
        }

        // Validate dense offsets and materialise the transaction actions
        // in a synchronous helper so the ref-struct `Span<T>` locals do
        // not need to be preserved across the `await` boundary below.
        ValidateDenseOffsets(treeId, shardIndex, offsets.Span);

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var firstOffset = offsets.Span[0];
        var endOffsetInclusive = firstOffset + offsets.Length - 1;
        var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, firstOffset);
        var phaseOneActions = BuildEncodedBatchActions(batchPartitionKey, encodedEntries.Span, offsets.Span);

        // Phase 0 / phase 1 / phase 2 split mirrors AppendBatchAsync;
        // see the comments there for the parallelism, candidate-row,
        // and monotonic-TAIL rationale.
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        var eliminateCandidateRow = _options.EliminateCandidateRowOnHotPath;
        var candidateTask = eliminateCandidateRow
            ? Task.CompletedTask
            : WriteCandidateRowAsync(
                table, manifestPartitionKey, firstOffset, endOffsetInclusive, cancellationToken);
        var phaseOneTask = SubmitPhaseOneAsync(table, phaseOneActions, treeId, shardIndex, cancellationToken);
        await candidateTask.ConfigureAwait(false);
        await phaseOneTask.ConfigureAwait(false);

        await DispatchPhaseTwoAsync(manifestPartitionKey, treeId, shardIndex, firstOffset, endOffsetInclusive, hasCandidateRow: !eliminateCandidateRow, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Submits a phase-1 transaction against the batch partition and
    /// records the wall-clock duration on
    /// <see cref="LatticeMetrics.ProviderCommitDuration"/>. Surfacing
    /// a retry exhaustion failure also emits
    /// <see cref="LatticeMetrics.ProviderRetryExhausted"/> with the
    /// HTTP status string so dashboards can attribute throttling /
    /// 429 / 5xx storms to the affected shard. Internal so the
    /// callers in <see cref="AppendBatchAsync"/> and
    /// <see cref="AppendEncodedBatchAsync"/> share a single timed
    /// path; not exposed beyond the provider.
    /// </summary>
    private async Task SubmitPhaseOneAsync(
        TableClient table,
        IReadOnlyList<TableTransactionAction> actions,
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, shardIndex);
        var startTicks = Stopwatch.GetTimestamp();
        try
        {
            await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LatticeMetrics.ProviderRetryExhausted.Add(1,
                treeTag,
                shardTag,
                LatticeMetrics.PhasePhase1Tag,
                new KeyValuePair<string, object?>(LatticeMetrics.TagStatus, ResolveProviderStatusTag(ex)));
            throw;
        }
        finally
        {
            var elapsedMs = Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds;
            // The pipeline-phase2 tag is identical for every call from
            // this provider instance (the option is read once at
            // construction), so the four-tag overload stays allocation-
            // free and the dashboards can still pivot the per-phase
            // duration series between sync and pipelined modes in a
            // single query.
            LatticeMetrics.ProviderCommitDuration.Record(elapsedMs,
                treeTag,
                shardTag,
                LatticeMetrics.PhasePhase1Tag,
                _pipelinePhaseTwoTag);
        }
    }

    /// <summary>
    /// Maps a provider exception to a low-cardinality
    /// <see cref="LatticeMetrics.TagStatus"/> tag value. The Azure
    /// Tables SDK surfaces transient and exhausted retries as
    /// <see cref="RequestFailedException"/>; everything else maps to
    /// the catch-all <c>unknown</c> bucket so the tag's cardinality
    /// stays bounded.
    /// </summary>
    private static string ResolveProviderStatusTag(Exception ex) => ex switch
    {
        RequestFailedException rfe => rfe.Status.ToString(CultureInfo.InvariantCulture),
        _ => "unknown",
    };

    /// <summary>
    /// Enqueues the phase-2 commit for the just-completed batch and
    /// awaits whichever phase-2 task the caller is supposed to block
    /// on per the configured durability mode.
    /// <para>
    /// <b>Default mode (<see cref="AzureTableWalStorageOptions.PipelinePhaseTwoCommits"/>
    /// is <see langword="false"/>).</b> Awaits the new batch's own
    /// phase-2 task. Post-append <see cref="GetHighestOffsetAsync"/>
    /// observes the new <c>TAIL</c>.
    /// </para>
    /// <para>
    /// <b>Pipelined mode (option <see langword="true"/>).</b>
    /// Atomically swaps the new batch's phase-2 task into the
    /// per-shard slot and awaits whatever the slot held before.
    /// That previous task is the previous append's phase-2 commit
    /// on the same shard; once it lands, the current call returns,
    /// even though the current batch's phase-2 is still in flight.
    /// A failed phase-2 surfaces on the next
    /// <see cref="AppendBatchAsync"/> on the same shard (the worker's
    /// sticky-failure semantics still hold because <c>WalShardGrain</c>
    /// resyncs <c>_nextOffset</c> on observed failure exactly as in
    /// the default mode). To guarantee surfacing even on a quiescent
    /// shard - the "last batch's phase-2 fault with no successor"
    /// gap - the slot occupant is also wired to
    /// <see cref="AzureTableWalStorageOptions.PipelinedPhaseTwoFaultHandler"/>
    /// via a one-shot continuation; see
    /// <see cref="AttachPipelinedFaultObserver(Task)"/>.
    /// </para>
    /// <para>
    /// <paramref name="cancellationToken"/> cancels only the current
    /// caller's wait on the predecessor's task (via
    /// <see cref="Task.WaitAsync(CancellationToken)"/>), never the
    /// predecessor's task itself - that task is shared state owned
    /// by the worker and any other observer (the next call, the
    /// fault-observer continuation, <see cref="DisposeAsync"/>) will
    /// continue to see it through to its terminal state.
    /// </para>
    /// </summary>
    private Task DispatchPhaseTwoAsync(
        string manifestPartitionKey,
        string treeId,
        int shardIndex,
        long firstOffset,
        long endOffsetInclusive,
        bool hasCandidateRow,
        CancellationToken cancellationToken)
    {
        var worker = GetOrCreatePhaseTwoWorker(treeId, shardIndex);
        var currentTask = worker.EnqueueAsync(firstOffset, endOffsetInclusive, hasCandidateRow);

        if (!_options.PipelinePhaseTwoCommits)
        {
            return cancellationToken.CanBeCanceled
                ? currentTask.WaitAsync(cancellationToken)
                : currentTask;
        }

        // The slot occupant must observe its own fault even if no
        // successor call ever arrives. AttachPipelinedFaultObserver
        // chains a one-shot continuation off currentTask before
        // currentTask enters the slot, so the configured handler
        // fires exactly once on fault regardless of who else awaits
        // the slot value (including DisposeAsync, which intentionally
        // swallows). The continuation runs on the thread pool with
        // ExecutionContext flow suppressed so it cannot re-enter the
        // request path or leak AsyncLocal state.
        var observed = AttachPipelinedFaultObserver(currentTask);
        return AwaitPreviousPipelinedAsync(manifestPartitionKey, observed, cancellationToken);
    }

    /// <summary>
    /// Wraps <paramref name="currentTask"/> with a one-shot
    /// fault-observation continuation routed to
    /// <see cref="AzureTableWalStorageOptions.PipelinedPhaseTwoFaultHandler"/>.
    /// The returned task is the same logical task the caller
    /// supplied; the continuation is fire-and-forget so it does not
    /// alter the task's settled value or timing relative to the
    /// pipelined slot's contract.
    /// <para>
    /// Returns <paramref name="currentTask"/> unchanged when no
    /// handler is configured, so the no-handler default path
    /// allocates nothing extra. When a handler is configured, the
    /// continuation is attached with
    /// <see cref="TaskContinuationOptions.OnlyOnFaulted"/> +
    /// <see cref="TaskContinuationOptions.ExecuteSynchronously"/>
    /// disabled, which queues to the thread pool only on fault and
    /// never on the success path.
    /// </para>
    /// <para>
    /// Internal so the test assembly can drive the helper directly
    /// without standing up Azurite.
    /// </para>
    /// </summary>
    internal Task AttachPipelinedFaultObserver(Task currentTask)
    {
        var handler = _options.PipelinedPhaseTwoFaultHandler;
        if (handler is null)
        {
            return currentTask;
        }

        // Suppress ExecutionContext so the continuation runs without
        // inheriting AsyncLocal state from the appending caller. The
        // continuation is shutdown-safe: it must not fault on its own
        // (handler exceptions are caught) and it accesses no captured
        // disposable state of the provider.
        using (ExecutionContext.SuppressFlow())
        {
            _ = currentTask.ContinueWith(
                static (t, state) =>
                {
                    var h = (Action<Exception>)state!;
                    var ex = t.Exception?.GetBaseException();
                    if (ex is null)
                    {
                        return;
                    }
                    try
                    {
                        h(ex);
                    }
                    catch
                    {
                        // The handler is for observability only; a
                        // throwing handler must not corrupt the
                        // pipeline's internal task graph.
                    }
                },
                handler,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.DenyChildAttach,
                TaskScheduler.Default);
        }

        return currentTask;
    }

    /// <summary>
    /// Exchanges the per-shard pipelined phase-2 task slot with
    /// <paramref name="currentTask"/> and awaits whatever was in the
    /// slot beforehand. Implemented as an explicit lock-free swap
    /// because <see cref="ConcurrentDictionary{TKey, TValue}"/> lacks
    /// an atomic exchange primitive that returns the previous value
    /// for an in-place update. Internal so the test assembly can
    /// pin the swap behaviour without driving the full append path.
    /// <para>
    /// <paramref name="cancellationToken"/> cancels only the wait on
    /// the predecessor task (via
    /// <see cref="Task.WaitAsync(CancellationToken)"/>), not the
    /// predecessor itself; the slot's task continues running and any
    /// other observer (next call, fault handler, dispose) will still
    /// see it through to its terminal state.
    /// </para>
    /// </summary>
    internal async Task AwaitPreviousPipelinedAsync(
        string manifestPartitionKey,
        Task currentTask,
        CancellationToken cancellationToken = default)
    {
        Task? previousTask;
        while (true)
        {
            if (_pipelinedPhaseTwoTasks.TryGetValue(manifestPartitionKey, out var existing))
            {
                if (_pipelinedPhaseTwoTasks.TryUpdate(manifestPartitionKey, currentTask, existing))
                {
                    previousTask = existing;
                    break;
                }
                // Lost the race; retry with the new comparand.
                continue;
            }

            if (_pipelinedPhaseTwoTasks.TryAdd(manifestPartitionKey, currentTask))
            {
                previousTask = null;
                break;
            }
            // Another thread added a slot between our TryGetValue
            // and TryAdd; retry the TryGetValue path.
        }

        if (previousTask is not null)
        {
            if (cancellationToken.CanBeCanceled)
            {
                await previousTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await previousTask.ConfigureAwait(false);
            }
        }
    }
    /// via <see cref="TableUpdateMode.Replace"/> on the upsert so a
    /// retry inside the SDK does not surface as <c>EntityAlreadyExists</c>
    /// to the caller; the row's identity is fully determined by
    /// <c>(manifestPartitionKey, startOffset)</c> and its payload
    /// (<c>endOffsetInclusive</c>) is deterministic for a given
    /// <c>(startOffset, entries.Count)</c>.
    /// </summary>
    private static async Task WriteCandidateRowAsync(
        TableClient table,
        string manifestPartitionKey,
        long startOffset,
        long endOffsetInclusive,
        CancellationToken cancellationToken)
    {
        var entity = new AzureTableWalEntity
        {
            PartitionKey = manifestPartitionKey,
            RowKey = BuildCandidateRowKey(startOffset),
            Offset = endOffsetInclusive,
            Payload = null,
        };
        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateDenseOffsets(string treeId, int shardIndex, ReadOnlySpan<long> offsetSpan)
    {
        var firstOffset = offsetSpan[0];
        if (firstOffset < 0L)
        {
            throw new ArgumentException(
                $"Append batch for '{treeId}/{shardIndex}' starts at a negative offset ({firstOffset}); WAL offsets are non-negative dense integers.",
                "offsets");
        }

        for (var i = 1; i < offsetSpan.Length; i++)
        {
            var expected = firstOffset + i;
            if (offsetSpan[i] != expected)
            {
                throw new ArgumentException(
                    $"Append batch for '{treeId}/{shardIndex}' is not dense: entry {i} has offset {offsetSpan[i]} but expected {expected}. "
                    + "Supplied offsets must equal offsets[0] + i for every i.",
                    "offsets");
            }
        }
    }

    private List<TableTransactionAction> BuildEncodedBatchActions(
        string partitionKey,
        ReadOnlySpan<ArraySegment<byte>> segments,
        ReadOnlySpan<long> offsetSpan)
    {
        var actions = new List<TableTransactionAction>(segments.Length);

        // Hand each segment straight to the row's Payload column - no
        // re-encode. ToArray() materialises the segment's bytes into a
        // freshly-owned byte[] so the entity's Payload field carries a
        // stable reference; the segment's underlying array is pooled
        // upstream by the WAL grain and will be returned to the pool
        // once the producer's batch completes.
        for (var i = 0; i < segments.Length; i++)
        {
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                new AzureTableWalEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = BuildEntryRowKey(offsetSpan[i]),
                    Offset = offsetSpan[i],
                    Payload = segments[i].AsSpan().ToArray(),
                }));
        }

        return actions;
    }

    /// <summary>
    /// Encodes <paramref name="entries"/> into <see cref="TableTransactionAction"/>
    /// add-actions appended onto <paramref name="actions"/>. Extracted from the
    /// <see cref="AppendBatchAsync"/> body so the per-entry encode hot path is
    /// callable from the bench host in-process without going through the Azure
    /// SDK transaction surface. Exposed <c>internal</c> for that single
    /// (bench-only) caller; production callers reach it through
    /// <see cref="AppendBatchAsync"/>.
    /// <para>
    /// A single <see cref="ArrayBufferWriter{T}"/> is allocated for the whole
    /// batch and reset between entries via <see cref="ArrayBufferWriter{T}.ResetWrittenCount"/>.
    /// The previous implementation allocated a fresh writer per entry; at the
    /// 99-entry transaction cap that materially dominated the encode-loop
    /// allocation profile (measured on the in-process WAL encode microbench).
    /// The shared writer's backing array is grown on demand as Orleans
    /// serialisation writes; subsequent entries reuse the grown capacity
    /// without reallocating until a payload exceeds the high-water mark.
    /// </para>
    /// </summary>
    internal void EncodeEntriesForBatch(
        string partitionKey,
        IReadOnlyList<WalEntry> entries,
        List<TableTransactionAction> actions)
    {
        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < entries.Count; i++)
        {
            writer.ResetWrittenCount();
            actions.Add(new TableTransactionAction(
                TableTransactionActionType.Add,
                BuildEntryEntity(partitionKey, entries[i], writer)));
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var firstWantedOffset = Math.Max(0L, fromOffsetExclusive + 1L);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        // Walk the manifest partition in ascending start-offset order.
        // Each manifest row's RowKey suffix carries the batch's
        // startOffset; the Offset column carries endOffsetInclusive.
        // We need every batch where endOffsetInclusive >= firstWantedOffset.
        // Manifest rows have a known D19 row-key layout so we can
        // skip batches that end strictly before firstWantedOffset by
        // filtering on Offset; the rest are yielded in order.
        var manifestFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}' and Offset ge {firstWantedOffset.ToString(CultureInfo.InvariantCulture)}";

        var yielded = 0;
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (yielded >= maxEntries)
            {
                yield break;
            }

            // Recover startOffset from the row key suffix - cheaper than
            // a second column round-trip and the schema fixes the
            // layout. The row key is `M{startOffset:D19}`.
            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var batchLowerInclusiveRowKey = BuildEntryRowKey(Math.Max(firstWantedOffset, startOffset));
            // Upper bound is strictly less than the first row beyond
            // the batch's endOffsetInclusive; use `lt 'F'` to bracket
            // the whole E-prefix space and rely on per-batch isolation
            // (every row in the partition is an entry row, no HEAD
            // sentinel in the new schema).
            var batchFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{batchLowerInclusiveRowKey}' and RowKey lt 'F'";

            await foreach (var entity in table
                .QueryAsync<AzureTableWalEntity>(batchFilter, maxPerPage: Math.Min(maxEntries - yielded, 1000), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (yielded >= maxEntries)
                {
                    yield break;
                }

                yield return new WalEntry
                {
                    Offset = entity.Offset,
                    Mutation = DeserialiseMutation(entity.Payload),
                };
                yielded++;
            }
        }
    }

    /// <summary>
    /// Bytes-shaped read override that hands the row <c>Payload</c>
    /// bytes back to the caller verbatim, skipping the
    /// <c>WalRecord</c> -&gt; <c>LatticeMutation</c> projection on
    /// the read path. Used by the shipper one-encode fast path to
    /// stream pre-encoded segments straight into the outbound framing
    /// encoder without an intermediate strongly-typed materialisation.
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="fromOffsetExclusive">Strict lower-bound offset; pass <c>-1</c> to read from the start of the log.</param>
    /// <param name="maxEntries">Maximum number of entries to yield; must be at least <c>1</c>.</param>
    /// <param name="encoder">Ignored on this override; the provider holds the encoded bytes verbatim. The argument is preserved on the signature to keep parity with the default fallback. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the scan and between every yielded row.</param>
    public async Task<WalShardEncodedPage> ReadEncodedAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var firstWantedOffset = Math.Max(0L, fromOffsetExclusive + 1L);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        var manifestFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}' and Offset ge {firstWantedOffset.ToString(CultureInfo.InvariantCulture)}";

        // Accumulate the byte segments and offsets in parallel. The
        // segments wrap each row's Payload array directly - one fewer
        // allocation per entry than the default fallback (which has
        // to ToArray() the encoder's WrittenSpan). The Payload arrays
        // are owned by the freshly-deserialised AzureTableWalEntity
        // instances, so they outlive the synchronous return of this
        // method.
        var segments = new List<ArraySegment<byte>>(Math.Min(maxEntries, 256));
        var offsets = new List<long>(Math.Min(maxEntries, 256));
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (segments.Count >= maxEntries)
            {
                break;
            }

            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var batchLowerInclusiveRowKey = BuildEntryRowKey(Math.Max(firstWantedOffset, startOffset));
            var batchFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{batchLowerInclusiveRowKey}' and RowKey lt 'F'";

            await foreach (var entity in table
                .QueryAsync<AzureTableWalEntity>(batchFilter, maxPerPage: Math.Min(maxEntries - segments.Count, 1000), cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (segments.Count >= maxEntries)
                {
                    break;
                }
                var payload = entity.Payload ?? Array.Empty<byte>();
                segments.Add(new ArraySegment<byte>(payload));
                offsets.Add(entity.Offset);
            }
        }

        var segmentsArray = segments.ToArray();
        var offsetsArray = offsets.ToArray();
        return new WalShardEncodedPage
        {
            EncodedEntries = segmentsArray,
            Offsets = offsetsArray,
            HighestOffsetInclusive = offsetsArray.Length == 0 ? -1L : offsetsArray[^1],
        };
    }

    /// <inheritdoc />
    public async Task<long> GetHighestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        try
        {
            // Point-read the per-shard TAIL row. The phase-2 worker
            // upserts TAIL inside the same atomic transaction that
            // adds the M{startOffset} row, so observing TAIL = X means
            // every batch with endOffsetInclusive <= X is durably
            // recorded in the manifest.
            var response = await table.GetEntityAsync<AzureTableWalEntity>(
                manifestPartitionKey,
                TailRowKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Value.Offset;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return -1L;
        }
    }

    /// <inheritdoc />
    public async Task<long> GetLowestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        // One Top(1) ascending scan of the manifest's M-rows. The row
        // key's D19 suffix sorts in numeric start-offset order so the
        // first match is the lowest committed batch start offset; that
        // batch's entry rows may have been partially trimmed, so the
        // live-low offset is the lowest extant entry-row offset inside
        // that batch partition - read it with a second Top(1).
        var manifestFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}'";
        long batchStartOffset = -1L;
        await foreach (var page in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, maxPerPage: 1, cancellationToken: cancellationToken)
            .AsPages(pageSizeHint: 1)
            .ConfigureAwait(false))
        {
            if (page.Values.Count > 0)
            {
                batchStartOffset = long.Parse(
                    page.Values[0].RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                    NumberStyles.None,
                    CultureInfo.InvariantCulture);
            }
            break;
        }

        if (batchStartOffset < 0L)
        {
            return -1L;
        }

        // Walk forward through manifest rows until we find one whose
        // batch partition still has entries (TrimAsync may have
        // deleted every entry in earlier batches but left the
        // manifest rows intact; the live-low offset must be sourced
        // from a non-empty batch).
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var entryFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey lt 'F'";
            await foreach (var entryPage in table
                .QueryAsync<AzureTableWalEntity>(entryFilter, maxPerPage: 1, cancellationToken: cancellationToken)
                .AsPages(pageSizeHint: 1)
                .ConfigureAwait(false))
            {
                if (entryPage.Values.Count > 0)
                {
                    return entryPage.Values[0].Offset;
                }
                break;
            }
        }

        return -1L;
    }

    /// <inheritdoc />
    public async Task TrimAsync(
        string treeId,
        int shardIndex,
        long throughOffsetInclusive,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (throughOffsetInclusive < 0L)
        {
            return;
        }

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);

        // Walk manifest rows in ascending start-offset order. For
        // batches that end at or before throughOffsetInclusive, delete
        // every entry in the batch partition plus the manifest row.
        // For the boundary batch (endOffset > throughOffsetInclusive
        // but startOffset <= throughOffsetInclusive), per-row delete
        // only the entries <= throughOffsetInclusive and leave the
        // manifest row in place so the boundary batch remains
        // discoverable. TAIL is never moved back by trim.
        var manifestFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}' and Offset le {throughOffsetInclusive.ToString(CultureInfo.InvariantCulture)}";

        var batchesFullyCovered = new List<long>();
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(manifestFilter, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);
            batchesFullyCovered.Add(startOffset);
        }

        foreach (var startOffset in batchesFullyCovered)
        {
            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var entryFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey lt 'F'";
            await DeletePartitionInChunksAsync(table, entryFilter, cancellationToken).ConfigureAwait(false);

            // Delete the manifest row once the batch's entries are
            // gone. Order matters for crash safety: if we crashed
            // between entry-delete and manifest-delete, the manifest
            // row would point at an empty batch partition - benign,
            // the next read just yields zero entries from it. The
            // reverse order would leave entry rows live but
            // unreachable from the manifest.
            await table.DeleteEntityAsync(
                manifestPartitionKey,
                BuildManifestRowKey(startOffset),
                ETag.All,
                cancellationToken).ConfigureAwait(false);
        }

        // Boundary batch: find the single batch whose startOffset <=
        // throughOffsetInclusive < endOffsetInclusive. There is at
        // most one such batch by manifest construction (batches are
        // contiguous in offset space).
        var boundaryFilter =
            $"PartitionKey eq '{Escape(manifestPartitionKey)}' and RowKey ge '{ManifestRowKeyPrefix}' and RowKey lt '{TailRowKey}' and Offset gt {throughOffsetInclusive.ToString(CultureInfo.InvariantCulture)}";
        await foreach (var manifestRow in table
            .QueryAsync<AzureTableWalEntity>(boundaryFilter, maxPerPage: 1, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var startOffset = long.Parse(
                manifestRow.RowKey.AsSpan(ManifestRowKeyPrefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture);
            if (startOffset > throughOffsetInclusive)
            {
                // The lowest still-extant manifest row already starts
                // past the trim point; nothing to do for the boundary.
                break;
            }

            var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, startOffset);
            var upperInclusiveRowKey = BuildEntryRowKey(throughOffsetInclusive);
            var entryFilter =
                $"PartitionKey eq '{Escape(batchPartitionKey)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey le '{upperInclusiveRowKey}'";
            await DeletePartitionInChunksAsync(table, entryFilter, cancellationToken).ConfigureAwait(false);
            break;
        }
    }

    private static async Task DeletePartitionInChunksAsync(
        TableClient table,
        string filter,
        CancellationToken cancellationToken)
    {
        // Stream the matching rows in pages and delete in transactional
        // chunks of MaxTransactionActions. Each chunk is its own
        // partition-scoped transaction, so a crash mid-trim leaves the
        // WAL in a valid state with a contiguous live tail; the next
        // trim call resumes from the new head.
        var pending = new List<TableTransactionAction>(MaxTransactionActions);
        await foreach (var entity in table
            .QueryAsync<AzureTableWalEntity>(filter, maxPerPage: MaxTransactionActions, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            pending.Add(new TableTransactionAction(
                TableTransactionActionType.Delete,
                entity,
                ETag.All));

            if (pending.Count == MaxTransactionActions)
            {
                await table.SubmitTransactionAsync(pending, cancellationToken).ConfigureAwait(false);
                pending.Clear();
            }
        }

        if (pending.Count > 0)
        {
            await table.SubmitTransactionAsync(pending, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the row-key for the entry at <paramref name="offset"/>.
    /// Exposed internally so tests can pin the layout without copying
    /// the format string.
    /// </summary>
    internal static string BuildEntryRowKey(long offset) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{EntryRowKeyPrefix}{offset:D19}");

    /// <summary>
    /// Builds the per-partition row-key for a <c>(treeId, shardIndex)</c>
    /// pair. Disallowed Azure Table partition-key characters
    /// (<c>'/'</c>, <c>'\'</c>, <c>'#'</c>, <c>'?'</c>, control bytes,
    /// and surrogates) are percent-encoded; <c>'%'</c> itself is also
    /// encoded so the function is round-trippable. Exposed internally
    /// for unit tests.
    /// </summary>
    internal static string BuildPartitionKey(string treeId, int shardIndex)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var encoded = EncodePartitionSegment(treeId);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{encoded}|{shardIndex}");
    }

    /// <summary>
    /// Builds the per-batch partition key for a single
    /// <see cref="AppendBatchAsync"/> call in the per-batch partition
    /// + manifest schema. The key is
    /// <c>{BatchPartitionPrefix}|{encoded-treeId}|{shardIndex}|S{startOffset:D19}</c>.
    /// The <c>S</c> infix sorts after the manifest's <c>M</c> rows
    /// lexicographically and the D19 width makes the partition keys
    /// inside a shard sort in start-offset order, so a tail scan can
    /// stream them with a single ascending-<c>PartitionKey</c> range
    /// query. Exposed internally for unit tests.
    /// </summary>
    internal static string BuildBatchPartitionKey(string treeId, int shardIndex, long startOffset)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (startOffset < 0L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startOffset),
                startOffset,
                "Batch partition keys are derived from WAL offsets, which are non-negative.");
        }
        var encoded = EncodePartitionSegment(treeId);
        // Interpolated under DefaultInterpolatedStringHandler: shardIndex
        // and the D19-formatted startOffset are written directly into the
        // handler's pooled char buffer via TryFormat, so the helper
        // allocates exactly one string (the final result) per call - no
        // intermediate ToString boxing, no string.Concat params array.
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{BatchPartitionPrefix}|{encoded}|{shardIndex}|S{startOffset:D19}");
    }

    /// <summary>
    /// Builds the per-shard manifest partition key in the per-batch
    /// partition + manifest schema. One manifest
    /// partition per shard, keyed as
    /// <c>{ManifestPartitionPrefix}|{encoded-treeId}|{shardIndex}</c>.
    /// Disjoint from <see cref="BuildBatchPartitionKey"/> by prefix
    /// (<c>_m_</c> vs <c>_b_</c>). Exposed internally for unit tests.
    /// </summary>
    internal static string BuildManifestPartitionKey(string treeId, int shardIndex)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var encoded = EncodePartitionSegment(treeId);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{ManifestPartitionPrefix}|{encoded}|{shardIndex}");
    }

    /// <summary>
    /// Builds the row-key for the manifest entry that records the
    /// batch starting at <paramref name="startOffset"/>. Format:
    /// <c>{ManifestRowKeyPrefix}{startOffset:D19}</c>. The D19 width
    /// makes row keys lexicographically equivalent to numeric
    /// start-offset order, so an ascending-<c>RowKey</c> range scan of
    /// a shard's manifest partition returns committed batches in
    /// commit-offset order. Exposed internally for unit tests.
    /// </summary>
    internal static string BuildManifestRowKey(long startOffset)
    {
        if (startOffset < 0L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startOffset),
                startOffset,
                "Manifest row keys are derived from WAL offsets, which are non-negative.");
        }
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{ManifestRowKeyPrefix}{startOffset:D19}");
    }

    /// <summary>
    /// Builds the candidate-row key for an in-flight batch starting at
    /// <paramref name="startOffset"/>. Format:
    /// <c>{CandidateRowKeyPrefix}{startOffset:D19}</c>. The C-row sits
    /// in the shard's manifest partition during phase 0 of an append
    /// and is deleted by phase 2 atomically with its M-row insert and
    /// the TAIL upsert. The D19 width makes C-row keys
    /// lexicographically equivalent to numeric start-offset order, so
    /// an ascending-<c>RowKey</c> range scan returns outstanding
    /// candidates in commit-offset order. Exposed internally for unit
    /// tests.
    /// </summary>
    internal static string BuildCandidateRowKey(long startOffset)
    {
        if (startOffset < 0L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startOffset),
                startOffset,
                "Candidate row keys are derived from WAL offsets, which are non-negative.");
        }
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{CandidateRowKeyPrefix}{startOffset:D19}");
    }

    private static string EncodePartitionSegment(string segment)
    {
        // Hot path: percent-encoded form is invariant for a given
        // segment string, so cache by ordinal identity. The cache is
        // bounded by the active-tree set and shared across every
        // provider instance.
        if (EncodedPartitionSegmentCache.TryGetValue(segment, out var cached))
        {
            return cached;
        }

        var encoded = EncodePartitionSegmentCore(segment);
        // First-writer-wins: a race only allocates one extra string
        // and the cache shape stabilises after the second call.
        return EncodedPartitionSegmentCache.GetOrAdd(segment, encoded);
    }

    private static string EncodePartitionSegmentCore(string segment)
    {
        // Conservative encoding: leave alphanumerics, '-', '_', '.'
        // alone; percent-encode everything else. Keeps the encoded form
        // valid as a partition key under Azure's documented rules and
        // round-trippable for diagnostics. UTF-8 byte-wise so non-ASCII
        // tree ids survive.
        //
        // Pure-ASCII fast path: scan the chars; if every char is in the
        // safe set we return the original string verbatim (no UTF-8
        // round-trip, no StringBuilder, no allocation beyond the cache
        // entry the caller stores). The canonical replication path
        // names tree ids with ASCII alphanumerics + '-' / '_' so this
        // path matches almost every production call.
        var fastPath = true;
        for (var i = 0; i < segment.Length; i++)
        {
            if (!IsSafeAsciiSegmentChar(segment[i]))
            {
                fastPath = false;
                break;
            }
        }
        if (fastPath)
        {
            return segment;
        }

        var utf8 = Encoding.UTF8.GetBytes(segment);
        var builder = new StringBuilder(utf8.Length);
        for (var i = 0; i < utf8.Length; i++)
        {
            var b = utf8[i];
            var safe = b is (>= (byte)'a' and <= (byte)'z')
                          or (>= (byte)'A' and <= (byte)'Z')
                          or (>= (byte)'0' and <= (byte)'9')
                          or (byte)'-' or (byte)'_' or (byte)'.';
            if (safe)
            {
                builder.Append((char)b);
            }
            else
            {
                builder.Append('%');
                builder.Append(b.ToString("X2", CultureInfo.InvariantCulture));
            }
        }
        return builder.ToString();
    }

    private static bool IsSafeAsciiSegmentChar(char c) =>
        c is (>= 'a' and <= 'z')
          or (>= 'A' and <= 'Z')
          or (>= '0' and <= '9')
          or '-' or '_' or '.';

    private static string Escape(string value) =>
        // OData filter literal escape: only the single quote needs
        // doubling. The PartitionKey we feed in here is already
        // percent-encoded so it never contains a single quote, but the
        // helper guards against future callers.
        value.Replace("'", "''", StringComparison.Ordinal);

    private AzureTableWalEntity BuildEntryEntity(string partitionKey, in WalEntry entry, ArrayBufferWriter<byte> buffer)
    {
        // Serialise via Orleans so the payload survives every
        // additive change to WalRecord under the existing Orleans-
        // serialization wire-compat rules. The provider does not need
        // to know the field layout - only that the bytes round-trip
        // through the same serializer. The caller hands in a buffer
        // that has already been reset to zero written count; we own it
        // for the duration of the call but never retain a reference
        // past the WrittenSpan.ToArray copy below.
        // The provider-boundary WalEntry carries the LatticeMutation-
        // shaped payload; project it to the durability-shaped WalRecord
        // so the on-disk format matches AppendEncodedBatchAsync exactly.
        // The legacy AppendBatchAsync path has no mode/origin context,
        // so we fall back to LwwRegister and an empty origin id (the
        // converter preserves the mutation's own origin when present).
        var record = WalRecordConverter.ToWalRecord(
            entry.Mutation,
            LatticeMergeMode.LwwRegister,
            string.Empty);
        _serializer.Serialize(record, buffer);
        return new AzureTableWalEntity
        {
            PartitionKey = partitionKey,
            RowKey = BuildEntryRowKey(entry.Offset),
            Offset = entry.Offset,
            Payload = buffer.WrittenSpan.ToArray(),
        };
    }

    private LatticeMutation DeserialiseMutation(byte[]? payload)
    {
        if (payload is null || payload.Length == 0)
        {
            // Defensive: a head-row payload should never reach this
            // path because the entry filter excludes RowKey 'HEAD'.
            // Returning a default mutation matches the in-memory
            // provider's behaviour for an absent shard.
            return default;
        }
        // On-disk format is WalRecord-shaped; project back to the
        // provider-boundary LatticeMutation shape so WalEntry.Mutation
        // consumers see the same surface they always have.
        var record = _serializer.Deserialize(new ReadOnlyMemory<byte>(payload));
        return WalRecordConverter.FromWalRecord(in record);
    }

    private async ValueTask<TableClient> EnsureTableAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _tableInitialised) == 1)
        {
            return _tableClient!;
        }

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _tableInitialised) == 1)
            {
                return _tableClient!;
            }

            var client = _options.BuildServiceClient().GetTableClient(_options.TableName);
            await client.CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
            _tableClient = client;
            Volatile.Write(ref _tableInitialised, 1);
            return client;
        }
        finally
        {
            _initLock.Release();
        }
    }

    internal PhaseTwoWorker GetOrCreatePhaseTwoWorker(string treeId, int shardIndex)
    {
        // Cache key matches the manifest partition layout but is held
        // as an ordinal string so the dictionary's hashing stays cheap.
        var manifestPartitionKey = BuildManifestPartitionKey(treeId, shardIndex);
        if (_phaseTwoWorkers.TryGetValue(manifestPartitionKey, out var existing))
        {
            return existing;
        }

        var created = new PhaseTwoWorker(
            EnsureTableAsync,
            manifestPartitionKey,
            treeId,
            shardIndex,
            _pipelinePhaseTwoTag);
        if (_phaseTwoWorkers.TryAdd(manifestPartitionKey, created))
        {
            return created;
        }

        // Lost the race; dispose our throwaway worker and use the
        // winner. DisposeAsync is fire-and-forget here because the
        // worker has done nothing yet (no commits enqueued).
        _ = created.DisposeAsync().AsTask();
        return _phaseTwoWorkers[manifestPartitionKey];
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            throw new ObjectDisposedException(nameof(AzureTableWalStorageProvider));
        }
    }

    /// <summary>
    /// Stops every per-shard phase-2 worker and awaits their drain
    /// loops. Pending phase-2 commits that have not yet been written
    /// are faulted with <see cref="ObjectDisposedException"/>; in-flight
    /// commits run to completion (the worker checks cancellation only
    /// between drains). Idempotent. Resolved by Orleans DI on silo
    /// shutdown via <see cref="IAsyncDisposable"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        // Snapshot the workers; new appends are rejected by
        // ThrowIfDisposed so no new workers can race in.
        var workers = _phaseTwoWorkers.Values.ToArray();
        _phaseTwoWorkers.Clear();

        // Drain any still-outstanding pipelined phase-2 tasks so a
        // host shutdown observes the same fault that a normal
        // post-append await would have. Faults are swallowed here
        // because Dispose is the terminal stage; the worker has
        // already surfaced them to its enqueued TCSs which the
        // pipelined tasks ultimately resolved against, and the
        // canonical surface for those faults is the next
        // AppendBatchAsync on the shard - which can no longer be
        // issued because the provider is disposed.
        var pipelined = _pipelinedPhaseTwoTasks.Values.ToArray();
        _pipelinedPhaseTwoTasks.Clear();
        foreach (var task in pipelined)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch
            {
                // See comment above; intentional swallow at shutdown.
            }
        }

        foreach (var worker in workers)
        {
            await worker.DisposeAsync().ConfigureAwait(false);
        }

        _initLock.Dispose();
    }
}
