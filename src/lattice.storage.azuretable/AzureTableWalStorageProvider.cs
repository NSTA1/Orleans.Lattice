using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Durable Azure Table Storage <see cref="IWalStorageProvider"/>. Uses
/// a two-phase per-batch / manifest schema (roadmap R-079) for true
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
/// one under burst load. <see cref="AppendBatchAsync"/> awaits the
/// phase-2 completion so post-append <see cref="GetHighestOffsetAsync"/>
/// observes the new TAIL.
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
public sealed class AzureTableWalStorageProvider : IWalStorageProvider, IAsyncDisposable
{
    /// <summary>
    /// Maximum number of <see cref="WalEntry"/> values that can be
    /// appended in a single <see cref="AppendBatchAsync"/> call. With
    /// the two-phase per-batch schema (roadmap R-079) phase 1 holds
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
    /// + manifest schema (roadmap R-079). Every <see cref="AppendBatchAsync"/>
    /// call lands in its own partition keyed as
    /// <c>{BatchPartitionPrefix}|{treeId}|{shardIndex}|S{startOffset:D19}</c>,
    /// giving concurrent appends true partition-server parallelism on
    /// the Azure Tables side. The leading marker is the minimal three
    /// bytes (<c>_b_</c>) so the partition key stays compact on every
    /// row (each row carries a copy on the wire); a longer marker like
    /// <c>__batch__</c> would add ~6 bytes per row across an entire
    /// shard's storage and network surface. The marker also makes the
    /// namespace disjoint from the manifest namespace
    /// (<see cref="ManifestPartitionPrefix"/>) and from the legacy
    /// single-partition schema (no marker), so the activation-time
    /// reconciliation step can distinguish all three by partition-key
    /// prefix alone.
    /// </summary>
    internal const string BatchPartitionPrefix = "_b_";

    /// <summary>
    /// Per-shard manifest partition prefix introduced by the per-batch
    /// partition + manifest schema (roadmap R-079). Each shard has
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

    private readonly AzureTableWalStorageOptions _options;
    private readonly Serializer<LatticeMutation> _serializer;
    private readonly ConcurrentDictionary<string, byte> _initialisedPartitions = new(StringComparer.Ordinal);

    // Per-shard phase-2 workers, lazily created on first append for a
    // given (treeId, shardIndex). Each worker owns a single Task plus
    // a bounded SortedSet drain buffer; shards are bounded by Orleans
    // activation counts so the steady-state overhead is bounded by
    // the silo's active-shard set.
    private readonly ConcurrentDictionary<string, PhaseTwoWorker> _phaseTwoWorkers =
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
        Serializer<LatticeMutation> serializer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);
        _options = options.Value ?? throw new ArgumentException(
            $"{nameof(IOptions<AzureTableWalStorageOptions>)}.{nameof(IOptions<AzureTableWalStorageOptions>.Value)} returned null.",
            nameof(options));
        _serializer = serializer;
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

        // Phase 1: write the entry rows into the batch's own partition
        // in a single transaction. Each batch hits a distinct Azure
        // Tables partition server so concurrent batches against the
        // same shard get true parallelism (the legacy single-partition
        // schema serialised them on one server).
        var batchPartitionKey = BuildBatchPartitionKey(treeId, shardIndex, firstOffset);
        var phaseOneActions = new List<TableTransactionAction>(entries.Count);
        EncodeEntriesForBatch(batchPartitionKey, entries, phaseOneActions);
        await table.SubmitTransactionAsync(phaseOneActions, cancellationToken).ConfigureAwait(false);

        // Phase 2: hand the (startOffset, endOffsetInclusive) pair to
        // the per-shard worker. The worker batches up to 99 phase-2
        // commits into one manifest-partition transaction in strict
        // ascending start-offset order, then upserts TAIL to the
        // group's highest endOffsetInclusive.
        var worker = GetOrCreatePhaseTwoWorker(treeId, shardIndex);
        await worker.EnqueueAsync(firstOffset, endOffsetInclusive).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalMutationEncoder encoder,
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

        // Phase 1 / phase 2 split mirrors AppendBatchAsync; see the
        // comments there for the parallelism + monotonic-TAIL
        // rationale.
        await table.SubmitTransactionAsync(phaseOneActions, cancellationToken).ConfigureAwait(false);

        var worker = GetOrCreatePhaseTwoWorker(treeId, shardIndex);
        await worker.EnqueueAsync(firstOffset, endOffsetInclusive).ConfigureAwait(false);
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
    /// + manifest schema (roadmap R-079). The key is
    /// <c>{BatchPartitionPrefix}|{encoded-treeId}|{shardIndex}|S{startOffset:D19}</c>.
    /// The <c>S</c> infix sorts after the manifest's <c>M</c> rows
    /// lexicographically and the D19 width makes the partition keys
    /// inside a shard sort in start-offset order, so a tail scan can
    /// stream them with a single ascending-<c>PartitionKey</c> range
    /// query. Disjoint from <see cref="BuildPartitionKey"/> by
    /// <c>|</c>-separator count (legacy = 1, batch = 3) so the legacy
    /// and new schemas can coexist transiently during the
    /// activation-time reconciliation step that rejects legacy data.
    /// Exposed internally for unit tests.
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
    /// partition + manifest schema (roadmap R-079). One manifest
    /// partition per shard, keyed as
    /// <c>{ManifestPartitionPrefix}|{encoded-treeId}|{shardIndex}</c>.
    /// Disjoint from <see cref="BuildBatchPartitionKey"/> by prefix
    /// (<c>_m_</c> vs <c>_b_</c>) and from <see cref="BuildPartitionKey"/>
    /// by <c>|</c>-separator count (legacy = 1, manifest = 2). Exposed
    /// internally for unit tests.
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

    private static string EncodePartitionSegment(string segment)
    {
        // Conservative encoding: leave alphanumerics, '-', '_', '.'
        // alone; percent-encode everything else. Keeps the encoded form
        // valid as a partition key under Azure's documented rules and
        // round-trippable for diagnostics. UTF-8 byte-wise so non-ASCII
        // tree ids survive.
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

    private static string Escape(string value) =>
        // OData filter literal escape: only the single quote needs
        // doubling. The PartitionKey we feed in here is already
        // percent-encoded so it never contains a single quote, but the
        // helper guards against future callers.
        value.Replace("'", "''", StringComparison.Ordinal);

    private AzureTableWalEntity BuildHeadEntity(string partitionKey, long highestOffset) => new()
    {
        PartitionKey = partitionKey,
        RowKey = HeadRowKey,
        Offset = highestOffset,
        Payload = null,
    };

    private AzureTableWalEntity BuildEntryEntity(string partitionKey, in WalEntry entry, ArrayBufferWriter<byte> buffer)
    {
        // Serialise via Orleans so the payload survives every
        // additive change to LatticeMutation under the existing
        // Orleans-serialization wire-compat rules. The provider does
        // not need to know the field layout - only that the bytes
        // round-trip through the same serializer. The caller hands in
        // a buffer that has already been reset to zero written count;
        // we own it for the duration of the call but never retain a
        // reference past the WrittenSpan.ToArray copy below.
        _serializer.Serialize(entry.Mutation, buffer);
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
        return _serializer.Deserialize(new ReadOnlyMemory<byte>(payload));
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

    private PhaseTwoWorker GetOrCreatePhaseTwoWorker(string treeId, int shardIndex)
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
            manifestPartitionKey);
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
        foreach (var worker in workers)
        {
            await worker.DisposeAsync().ConfigureAwait(false);
        }

        _initLock.Dispose();
    }
}
