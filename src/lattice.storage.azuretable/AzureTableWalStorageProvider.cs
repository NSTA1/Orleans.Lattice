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
/// Durable Azure Table Storage <see cref="IWalStorageProvider"/>. One
/// Azure-Table partition per <c>(treeId, shardIndex)</c> pair; one
/// table row per appended <see cref="WalEntry"/> plus a per-partition
/// head-pointer sentinel that lets <see cref="GetHighestOffsetAsync"/>
/// resolve in a single point read.
/// <para>
/// <b>Atomicity.</b> Every <see cref="AppendBatchAsync"/> call is
/// translated to a single
/// <see cref="TableClient.SubmitTransactionAsync"/> within the target
/// partition. The transaction contains one upsert against the head
/// sentinel followed by one add per appended entry. Azure Tables
/// commits the transaction atomically across the partition or fails
/// the whole batch, satisfying the
/// <see cref="IWalStorageProvider.AppendBatchAsync"/> all-or-nothing
/// contract.
/// </para>
/// <para>
/// <b>Capacity.</b> Azure Tables caps a single transaction at 100
/// actions and 4&#160;MiB. Because every batch reserves one action for
/// the head upsert, the provider rejects batches of more than
/// <see cref="MaxEntriesPerBatch"/> entries with
/// <see cref="ArgumentException"/>. Callers (the WAL grain) therefore
/// chunk larger batches before invoking the provider; the upstream
/// <c>LatticeReplicationOptions.MaxBatchSize</c> already keeps batches
/// well below this cap in the canonical replication path.
/// </para>
/// <para>
/// <b>Thread safety.</b> Instances are safe for concurrent calls
/// across distinct partitions. Concurrent calls targeting the same
/// partition rely on Azure Tables' partition-level transactional
/// serialisation - and on the head-pointer upsert seeing the new
/// transaction's highest offset rather than a stale earlier one.
/// The WAL grain is single-writer per shard, so concurrent calls into
/// the same partition only happen when
/// <see cref="Orleans.Lattice.LatticeOptions.WalMaxPendingBatches"/>
/// is greater than 1; in that mode the smaller-offset transaction can
/// race the larger-offset one and clobber the head pointer. Hosts that
/// raise <c>WalMaxPendingBatches</c> against this provider must accept
/// that <see cref="GetHighestOffsetAsync"/> may briefly report a stale
/// value until every concurrent transaction settles. The default cap
/// of <c>1</c> avoids the race entirely.
/// </para>
/// </summary>
public sealed class AzureTableWalStorageProvider : IWalStorageProvider
{
    /// <summary>
    /// Maximum number of <see cref="WalEntry"/> values that can be
    /// appended in a single <see cref="AppendBatchAsync"/> call. One
    /// of the 100 transaction actions is reserved for the head-pointer
    /// upsert, leaving 99 for entries.
    /// </summary>
    public const int MaxEntriesPerBatch = 99;

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

    private const int MaxTransactionActions = 100;

    private readonly AzureTableWalStorageOptions _options;
    private readonly Serializer<LatticeMutation> _serializer;
    private readonly ConcurrentDictionary<string, byte> _initialisedPartitions = new(StringComparer.Ordinal);

    private TableClient? _tableClient;
    private int _tableInitialised;

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

        if (entries.Count == 0)
        {
            return;
        }

        if (entries.Count > MaxEntriesPerBatch)
        {
            throw new ArgumentException(
                $"Azure Table Storage caps a single transactional batch at {MaxTransactionActions} actions and the provider reserves one for the head-pointer upsert; "
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
        var partitionKey = BuildPartitionKey(treeId, shardIndex);

        var actions = new List<TableTransactionAction>(entries.Count + 1)
        {
            new(
                TableTransactionActionType.UpsertReplace,
                BuildHeadEntity(partitionKey, highestOffset: firstOffset + entries.Count - 1)),
        };

        EncodeEntriesForBatch(partitionKey, entries, actions);

        await table.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
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
        var partitionKey = BuildPartitionKey(treeId, shardIndex);

        // First strictly-greater-than offset is fromOffsetExclusive + 1
        // (clamped at 0 so a -1 sentinel reads from the start). Build a
        // tight RowKey range query that hits only entry rows in this
        // partition.
        var firstWantedOffset = Math.Max(0L, fromOffsetExclusive + 1L);
        var lowerInclusiveRowKey = BuildEntryRowKey(firstWantedOffset);
        var filter = $"PartitionKey eq '{Escape(partitionKey)}' and RowKey ge '{lowerInclusiveRowKey}' and RowKey lt '{HeadRowKey}'";

        var yielded = 0;
        await foreach (var entity in table
            .QueryAsync<AzureTableWalEntity>(filter, maxPerPage: Math.Min(maxEntries, 1000), cancellationToken: cancellationToken)
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

    /// <inheritdoc />
    public async Task<long> GetHighestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var table = await EnsureTableAsync(cancellationToken).ConfigureAwait(false);
        var partitionKey = BuildPartitionKey(treeId, shardIndex);

        try
        {
            var response = await table.GetEntityAsync<AzureTableWalEntity>(
                partitionKey,
                HeadRowKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Value.Offset;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return -1L;
        }
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
        var partitionKey = BuildPartitionKey(treeId, shardIndex);
        var upperInclusiveRowKey = BuildEntryRowKey(throughOffsetInclusive);
        var filter = $"PartitionKey eq '{Escape(partitionKey)}' and RowKey ge '{EntryRowKeyPrefix}' and RowKey le '{upperInclusiveRowKey}'";

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
        EntryRowKeyPrefix + offset.ToString("D19", CultureInfo.InvariantCulture);

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
        return encoded + "|" + shardIndex.ToString(CultureInfo.InvariantCulture);
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
}
