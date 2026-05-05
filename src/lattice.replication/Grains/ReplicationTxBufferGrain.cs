using System.Globalization;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree atomic-batch staging buffer grain. See
/// <see cref="IReplicationTxBufferGrain"/> for the contract.
/// <para>
/// Storage is delegated to a reserved system tree named
/// <c>_lattice_replog_txbuf_{treeId}</c> resolved through the internal
/// <see cref="ISystemLattice"/> surface, so the buffer inherits the
/// scaling, sharding, and persistence of the core B+ tree rather than
/// living inside a single grain's persistent-state row. A silo crash
/// mid-batch therefore does not lose staged entries — the grain
/// reactivates on a surviving silo and rehydrates the in-memory
/// index from the system tree.
/// </para>
/// <para>
/// Row-key shape: <c>"b/{originClusterId}/{transactionId-N}/{index-D10}"</c>,
/// where <c>transactionId-N</c> is the canonical
/// <see cref="Guid.ToString(string)"/> "N" form (32 lowercase hex
/// digits, no hyphens) and <c>index-D10</c> is a 10-digit zero-padded
/// integer. The shape preserves cheap range-scan recovery — every
/// entry for a given <c>(origin, txid)</c> pair is contiguous in
/// lexicographic order — and the prefix <c>"b/"</c> is reserved
/// exclusively for staged-entry rows so the activation walk over the
/// system tree can scope to the staged entries cleanly.
/// </para>
/// <para>
/// Eviction policy: when admission would push the in-flight
/// transaction count past
/// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>
/// or the cumulative payload past
/// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxBytes"/>,
/// the oldest partially-buffered transaction (FIFO by the
/// <see cref="TxStagedEntry.EnqueuedAtTicks"/> of its first staged
/// entry) is evicted to make room. Eviction surfaces every displaced
/// entry through the per-tree dead-letter queue tagged
/// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>; the
/// producer's per-origin high-water-mark was never advanced past
/// the displaced entries so they remain durably re-shippable.
/// </para>
/// </summary>
internal sealed class ReplicationTxBufferGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    Serializer<TxStagedEntry> serializer) : IReplicationTxBufferGrain, IGrainBase
{
    /// <summary>Inclusive prefix every staged-entry key carries inside the system tree.</summary>
    private const string EntryKeyPrefix = "b/";

    /// <summary>Exclusive end key for a prefix range scan over <see cref="EntryKeyPrefix"/>.</summary>
    /// <remarks>ASCII '/' (0x2F) &lt; '0' (0x30); "b0" is therefore strictly greater than every "b/..." key.</remarks>
    private const string EntryKeyPrefixEnd = "b0";

    /// <summary>Width of the zero-padded batch-index segment in stored keys.</summary>
    private const int IndexWidth = 10;

    /// <summary>
    /// Per-entry size overhead applied on top of the value length when
    /// estimating the buffer's cumulative byte cost. Conservatively
    /// covers the row-key string plus the Orleans-serializer envelope
    /// for a single <see cref="TxStagedEntry"/>.
    /// </summary>
    internal const int PerEntryByteOverhead = 256;

    private string _treeId = "";
    private ISystemLattice _store = null!;

    /// <summary>
    /// In-memory index: per-<c>(origin, txid)</c> dictionary mapping
    /// each staged batch index to the staged entry. The dictionary
    /// reaches <see cref="ReplogEntry.AtomicBatchSize"/> entries when
    /// the batch is complete.
    /// </summary>
    private readonly Dictionary<TransactionKey, Dictionary<int, TxStagedEntry>> _byTransaction = new();

    /// <summary>FIFO of transaction keys in admission order (used by the eviction policy).</summary>
    private readonly LinkedList<TransactionKey> _admissionOrder = new();

    /// <summary>Index back into <see cref="_admissionOrder"/> so removal is O(1).</summary>
    private readonly Dictionary<TransactionKey, LinkedListNode<TransactionKey>> _admissionNodes = new();

    /// <summary>Cumulative tracked bytes across every staged entry.</summary>
    private long _trackedBytes;

    private bool _initialized;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationTxBufferGrain)} activation key is empty; expected the replicated tree id.");
        }

        _treeId = key;
        _store = grainFactory.GetGrain<ISystemLattice>(BackingTreeId(_treeId));

        await BulkLoadAsync(cancellationToken).ConfigureAwait(true);
        _initialized = true;
    }

    /// <summary>
    /// Test-only initialisation seam. Bypasses Orleans activation by
    /// supplying the tree id and pre-bound <see cref="ISystemLattice"/>
    /// store directly, then running the same bulk-load logic
    /// <see cref="OnActivateAsync(CancellationToken)"/> uses. Tests
    /// that exercise the grain in isolation use this in lieu of the
    /// activation lifecycle.
    /// </summary>
    internal async Task InitializeForTestingAsync(
        string treeId,
        ISystemLattice store,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(store);

        _treeId = treeId;
        _store = store;
        _byTransaction.Clear();
        _admissionOrder.Clear();
        _admissionNodes.Clear();
        _trackedBytes = 0;

        await BulkLoadAsync(cancellationToken).ConfigureAwait(true);
        _initialized = true;
    }

    private async Task BulkLoadAsync(CancellationToken cancellationToken)
    {
        await foreach (var kvp in _store.EntriesAsync(
            startInclusive: EntryKeyPrefix,
            endExclusive: EntryKeyPrefixEnd,
            cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            TxStagedEntry staged;
            try
            {
                staged = serializer.Deserialize(kvp.Value);
            }
            catch
            {
                // A malformed row is skipped rather than crashing
                // activation. Defensive: in production every row is
                // written by this grain, so deserialization failures
                // would only occur on a corrupted backing store.
                continue;
            }

            AdmitInMemory(staged, isRehydration: true);
        }
    }

    /// <inheritdoc />
    public async Task<TxBufferAdmissionResult> AdmitAsync(ReplogEntry entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        if (string.IsNullOrEmpty(entry.OriginClusterId))
        {
            throw new ArgumentException(
                "ReplogEntry.OriginClusterId must be non-empty for atomic-batch staging admission.",
                nameof(entry));
        }

        if (entry.AtomicBatchSize <= 0)
        {
            throw new ArgumentException(
                $"ReplogEntry.AtomicBatchSize must be positive for atomic-batch staging admission; got {entry.AtomicBatchSize}.",
                nameof(entry));
        }

        if (entry.AtomicBatchIndex < 0 || entry.AtomicBatchIndex >= entry.AtomicBatchSize)
        {
            throw new ArgumentException(
                $"ReplogEntry.AtomicBatchIndex {entry.AtomicBatchIndex} is outside [0, {entry.AtomicBatchSize}).",
                nameof(entry));
        }

        if (entry.TransactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "ReplogEntry.TransactionId must be non-empty for atomic-batch staging admission.",
                nameof(entry));
        }

        var key = new TransactionKey(entry.OriginClusterId!, entry.TransactionId);

        // Idempotent re-delivery: a producer re-shipping after a
        // transient receiver failure must not inflate the buffer.
        if (_byTransaction.TryGetValue(key, out var existing) && existing.ContainsKey(entry.AtomicBatchIndex))
        {
            return new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = true,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
            };
        }

        // Evict to make room before persisting so the eviction's DLQ
        // routing and the persistent write succeed-or-fail together.
        var resolved = optionsMonitor.Get(_treeId);
        await EvictUntilCapacityAsync(resolved, entry, cancellationToken).ConfigureAwait(true);

        var staged = new TxStagedEntry
        {
            OriginClusterId = entry.OriginClusterId!,
            TransactionId = entry.TransactionId,
            BatchSize = entry.AtomicBatchSize,
            BatchIndex = entry.AtomicBatchIndex,
            Entry = entry,
            EnqueuedAtTicks = DateTime.UtcNow.Ticks,
        };

        var encoded = serializer.SerializeToArray(staged);
        await _store.SetAsync(EntryKey(staged), encoded, cancellationToken).ConfigureAwait(true);

        AdmitInMemory(staged, isRehydration: false);

        if (_byTransaction.TryGetValue(key, out var siblings) && siblings.Count == staged.BatchSize)
        {
            // Batch complete: surface the in-canonical-order list to
            // the caller and remove the in-memory + persistent state
            // so a follow-up re-delivery of one of the siblings
            // dedupes against an empty buffer rather than re-firing
            // completion.
            var completed = new TxStagedEntry[siblings.Count];
            for (var i = 0; i < siblings.Count; i++)
            {
                completed[i] = siblings[i];
            }

            await RemoveTransactionAsync(key, deleteFromStore: true, cancellationToken).ConfigureAwait(true);

            return new TxBufferAdmissionResult
            {
                BatchComplete = true,
                Deduped = false,
                CompletedBatch = completed,
            };
        }

        return new TxBufferAdmissionResult
        {
            BatchComplete = false,
            Deduped = false,
            CompletedBatch = Array.Empty<TxStagedEntry>(),
        };
    }

    /// <inheritdoc />
    public Task<int> CountTransactionsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_byTransaction.Count);
    }

    /// <inheritdoc />
    public Task<long> CountBytesAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_trackedBytes);
    }

    private void AdmitInMemory(TxStagedEntry staged, bool isRehydration)
    {
        var key = new TransactionKey(staged.OriginClusterId, staged.TransactionId);
        if (!_byTransaction.TryGetValue(key, out var siblings))
        {
            siblings = new Dictionary<int, TxStagedEntry>();
            _byTransaction[key] = siblings;
            var node = _admissionOrder.AddLast(key);
            _admissionNodes[key] = node;
        }

        if (siblings.ContainsKey(staged.BatchIndex))
        {
            // Defensive on rehydration: a duplicated row in the
            // backing store (should not occur in practice) is
            // tolerated as a no-op.
            return;
        }

        siblings[staged.BatchIndex] = staged;
        _trackedBytes += EstimateBytes(staged);
    }

    private async Task EvictUntilCapacityAsync(
        LatticeReplicationOptions resolved,
        ReplogEntry incoming,
        CancellationToken cancellationToken)
    {
        var key = new TransactionKey(incoming.OriginClusterId!, incoming.TransactionId);

        // Transaction-count cap: only count transactions other than the
        // one being admitted, because admitting an entry into an
        // already-tracked transaction does not grow the in-flight
        // count.
        var growsTransactionCount = !_byTransaction.ContainsKey(key);
        while (growsTransactionCount && _byTransaction.Count >= resolved.AtomicBatchBufferMaxTransactions)
        {
            if (!await EvictOldestAsync(cancellationToken).ConfigureAwait(true))
            {
                break;
            }
            growsTransactionCount = !_byTransaction.ContainsKey(key);
        }

        // Byte cap: estimate the cost of admitting the new entry and
        // evict until adding it would not exceed the cap. A single
        // entry larger than the cap is admitted as-is rather than
        // evicting the entire buffer (the cap is guidance, not a
        // per-entry hard limit).
        var incomingBytes = EstimateBytes(incoming);
        while (_byTransaction.Count > 0
            && _trackedBytes + incomingBytes > resolved.AtomicBatchBufferMaxBytes)
        {
            if (!await EvictOldestAsync(cancellationToken).ConfigureAwait(true))
            {
                break;
            }
        }
    }

    private async Task<bool> EvictOldestAsync(CancellationToken cancellationToken)
    {
        var node = _admissionOrder.First;
        if (node is null)
        {
            return false;
        }

        var oldest = node.Value;
        if (!_byTransaction.TryGetValue(oldest, out var siblings))
        {
            // Defensive: the index is out of sync. Drop the FIFO node
            // and retry.
            _admissionOrder.RemoveFirst();
            _admissionNodes.Remove(oldest);
            return true;
        }

        // Snapshot the displaced entries so the DLQ enqueue can run
        // after we have already removed them from the in-memory index
        // (avoids re-emitting them if the DLQ call itself reactivates
        // the buffer through some unrelated path).
        var displaced = siblings.Values.ToArray();
        await RemoveTransactionAsync(oldest, deleteFromStore: true, cancellationToken).ConfigureAwait(true);

        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeId);
        foreach (var staged in displaced)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await dlq.EnqueueAsync(
                    staged.Entry,
                    failureReason: "Atomic-batch staging buffer full; evicted partially-buffered transaction to make room.",
                    retryCount: 0,
                    reasonTag: LatticeReplicationMetrics.ReasonEvicted,
                    cancellationToken).ConfigureAwait(true);
            }
            catch
            {
                // Best-effort DLQ routing: the WAL still holds the
                // originals because the per-origin high-water-mark
                // was never advanced past the displaced entries, so
                // they remain durably re-shippable on the next
                // producer pump cycle. Swallow the DLQ failure rather
                // than blocking the eviction.
            }
        }

        return true;
    }

    private async Task RemoveTransactionAsync(
        TransactionKey key,
        bool deleteFromStore,
        CancellationToken cancellationToken)
    {
        if (!_byTransaction.Remove(key, out var siblings))
        {
            return;
        }

        if (_admissionNodes.Remove(key, out var node))
        {
            _admissionOrder.Remove(node);
        }

        long releasedBytes = 0;
        foreach (var staged in siblings.Values)
        {
            releasedBytes += EstimateBytes(staged);
        }
        _trackedBytes -= releasedBytes;
        if (_trackedBytes < 0)
        {
            // Defensive clamp: should never trip in production but
            // protects observability if an estimate drifts.
            _trackedBytes = 0;
        }

        if (deleteFromStore)
        {
            foreach (var staged in siblings.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await _store.DeleteAsync(EntryKey(staged), cancellationToken).ConfigureAwait(true);
            }
        }
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationTxBufferGrain)} for tree '{_treeId}' has not completed activation.");
        }
    }

    private static int EstimateBytes(TxStagedEntry staged) =>
        EstimateBytes(staged.Entry);

    private static int EstimateBytes(ReplogEntry entry)
    {
        var valueLength = entry.Value?.Length ?? 0;
        return PerEntryByteOverhead + valueLength;
    }

    /// <summary>
    /// Composes the system-tree id used to back the atomic-batch staging
    /// buffer for <paramref name="treeId"/>. Lives inside the reserved
    /// <c>_lattice_replog_</c> namespace so user trees cannot collide
    /// with it.
    /// </summary>
    internal static string BackingTreeId(string treeId) =>
        $"{LatticeConstants.ReplogTreePrefix}txbuf_{treeId}";

    /// <summary>
    /// Builds the system-tree row key for a staged entry. Format is
    /// <c>"b/{originClusterId}/{transactionId-N}/{index-D10}"</c>.
    /// Range scans over the <c>"b/"</c> prefix recover every staged
    /// entry on activation.
    /// </summary>
    internal static string EntryKey(TxStagedEntry staged) =>
        EntryKey(staged.OriginClusterId, staged.TransactionId, staged.BatchIndex);

    internal static string EntryKey(string originClusterId, Guid transactionId, int batchIndex) =>
        string.Concat(
            EntryKeyPrefix,
            originClusterId,
            "/",
            transactionId.ToString("N", CultureInfo.InvariantCulture),
            "/",
            batchIndex.ToString("D" + IndexWidth, CultureInfo.InvariantCulture));

    /// <summary>
    /// Composite key for the in-memory transaction index. Equality is
    /// ordinal on the origin string and standard on the GUID, matching
    /// the wire-shape identity tuple receivers compare against the
    /// per-origin high-water-mark.
    /// </summary>
    private readonly record struct TransactionKey(string OriginClusterId, Guid TransactionId);
}
