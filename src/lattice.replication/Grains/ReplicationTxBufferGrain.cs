using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
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
    Serializer<TxStagedEntry> serializer,
    ILatticeReplicationCursorRegistry? cursorRegistry = null,
    ILogger<ReplicationTxBufferGrain>? logger = null) : IReplicationTxBufferGrain, IGrainBase
{
    private readonly ILogger<ReplicationTxBufferGrain> _logger =
        logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<ReplicationTxBufferGrain>.Instance;

    /// <summary>Inclusive prefix every staged-entry key carries inside the system tree.</summary>
    private const string EntryKeyPrefix = "b/";

    /// <summary>Exclusive end key for a prefix range scan over <see cref="EntryKeyPrefix"/>.</summary>
    /// <remarks>ASCII '/' (0x2F) &lt; '0' (0x30); "b0" is therefore strictly greater than every "b/..." key.</remarks>
    private const string EntryKeyPrefixEnd = "b0";

    /// <summary>
    /// Inclusive prefix every blacklist row key carries inside the
    /// system tree. Disjoint from <see cref="EntryKeyPrefix"/>
    /// (ASCII 'b' (0x62) &lt; 'x' (0x78)) so the staged-entry and
    /// blacklist range scans are independent and either order
    /// produces the same in-memory state on rehydration.
    /// </summary>
    private const string BlacklistKeyPrefix = "x/";

    /// <summary>Exclusive end key for a prefix range scan over <see cref="BlacklistKeyPrefix"/>.</summary>
    /// <remarks>ASCII '/' (0x2F) &lt; '0' (0x30); "x0" is therefore strictly greater than every "x/..." key.</remarks>
    private const string BlacklistKeyPrefixEnd = "x0";

    /// <summary>Single-byte sentinel value stored under each blacklist row; the row's existence is the membership signal.</summary>
    private static readonly byte[] BlacklistRowValue = new byte[] { 0 };

    /// <summary>
    /// Composes the system-tree row key for a single blacklisted
    /// transaction id. The "N" Guid format is canonical lowercase
    /// 32-hex-digit no-hyphen — stable across every silo and
    /// runtime so a rehydration on a different silo decodes to the
    /// same id.
    /// </summary>
    private static string BlacklistKey(Guid transactionId) =>
        BlacklistKeyPrefix + transactionId.ToString("N");

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

    /// <summary>
    /// Set of transaction keys that were admitted via activation
    /// rehydration rather than a live <see cref="AdmitAsync(ReplogEntry, CancellationToken)"/>
    /// call. Tracked so the matching decrement in
    /// <see cref="RemoveTransactionAsync(TransactionKey, bool, CancellationToken)"/>
    /// can be skipped for these keys: the admit-time
    /// <see cref="LatticeReplicationMetrics.ApplyTxBuffered"/> /
    /// <see cref="LatticeReplicationMetrics.ApplyTxBufferBytes"/>
    /// increments were intentionally suppressed (the gauge contract
    /// is "live admission lifecycle, not durable buffer occupancy"),
    /// so a paired decrement on removal would push the gauge below
    /// the activation's true live volume — visible to operators as
    /// a negative reading after every silo restart that rehydrated
    /// a non-empty buffer. Membership is removed when the key is
    /// removed from the buffer for any reason; a transaction that
    /// rehydrated and later receives a fresh live <c>AdmitAsync</c>
    /// for a *new* batch index does not leave the set, because the
    /// admit-on-existing-key path is a no-op on the gauge (only the
    /// first admit of a new transaction key contributes), and the
    /// terminal removal still maps to the rehydrated admit.
    /// </summary>
    private readonly HashSet<TransactionKey> _rehydratedKeys = new();

    /// <summary>
    /// Sorted multiset of staged HLCs supporting O(log N) lookup of
    /// the lowest staged HLC for the producer-side blocked-floor GC
    /// pin. Each staged entry contributes its <see cref="ReplogEntry.Timestamp"/>
    /// as a key; the value is the reference count (multiple entries
    /// can share the same HLC if a producer stamps siblings of an
    /// atomic batch with the same source HLC). Maintained
    /// incrementally on every admit / removal so
    /// <see cref="GetLowestStagedHlcAsync(CancellationToken)"/>
    /// resolves in O(log N) rather than scanning the full
    /// <see cref="_byTransaction"/> graph (worst-case
    /// O(<see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>
    /// × per-batch siblings)).
    /// </summary>
    private readonly SortedDictionary<HybridLogicalClock, int> _stagedHlcCounts =
        new(HlcComparer.Instance);

    /// <summary>
    /// Comparer used to drive <see cref="_stagedHlcCounts"/>.
    /// <see cref="HybridLogicalClock"/> exposes
    /// <see cref="HybridLogicalClock.CompareTo(HybridLogicalClock)"/>
    /// but does not declare
    /// <see cref="IComparable{T}"/> in its base list, so the default
    /// <see cref="Comparer{T}"/> for the type cannot be resolved.
    /// Providing an explicit comparer keeps the sorted multiset
    /// O(log N) without a public-API change to the primitive.
    /// </summary>
    private sealed class HlcComparer : IComparer<HybridLogicalClock>
    {
        public static readonly HlcComparer Instance = new();
        public int Compare(HybridLogicalClock x, HybridLogicalClock y) => x.CompareTo(y);
    }

    /// <summary>Cumulative tracked bytes across every staged entry.</summary>
    private long _trackedBytes;

    private bool _initialized;

    /// <summary>
    /// Atomic-batch saga transaction ids registered via
    /// <see cref="RegisterBlacklistedTransactionsAsync(IReadOnlyList{Guid}, CancellationToken)"/>.
    /// Admission for any of these ids short-circuits to
    /// <see cref="TxBufferAdmissionResult.BlacklistedBypass"/> set
    /// to <c>true</c> with no entry staged.
    /// <para>
    /// Persisted to the per-tree backing system tree under
    /// <see cref="BlacklistKeyPrefix"/> (one row per id, sentinel
    /// value, key shape <c>"x/{transactionId-N}"</c>) so a silo
    /// crash mid-bootstrap does not silently disable the bypass
    /// path on the next activation. Without persistence a buffer
    /// grain that deactivated under steady-state load would
    /// re-stage subsequent incremental entries for blacklisted
    /// sagas, which can never reach completeness — the orphan
    /// timeout would eventually evict them, but the bypass-then-
    /// point-apply contract would be silently disabled until
    /// then. <see cref="BulkLoadAsync"/> rehydrates the in-memory
    /// set from the same prefix range on activation.
    /// </para>
    /// </summary>
    private readonly HashSet<Guid> _blacklistedTransactions = new();

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

        await RepublishRehydratedFloorAsync(cancellationToken).ConfigureAwait(true);
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
        _stagedHlcCounts.Clear();
        _trackedBytes = 0;

        await BulkLoadAsync(cancellationToken).ConfigureAwait(true);
        _initialized = true;

        await RepublishRehydratedFloorAsync(cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Republishes the lowest staged HLC to the cursor registry after
    /// activation rehydrates the in-memory index from the backing
    /// system tree. Without this republish, a silo restart that loses
    /// the in-process registry state (the default
    /// <see cref="InMemoryReplicationCursorRegistry"/> is per-silo,
    /// not durable) would silently drop the producer-side blocked-floor
    /// GC pin until the next admit or removal call — and a producer
    /// running ahead of the receiver could trim the WAL through HLCs
    /// the buffer is still staging.
    /// <para>
    /// Failures are logged at Warning level and swallowed: the next
    /// admit / remove call will reattempt the publish through the
    /// applier's standard reporting path, so a transient registry
    /// failure does not block activation.
    /// </para>
    /// </summary>
    private async Task RepublishRehydratedFloorAsync(CancellationToken cancellationToken)
    {
        if (cursorRegistry is null || _stagedHlcCounts.Count == 0)
        {
            return;
        }

        HybridLogicalClock floor;
        using (var enumerator = _stagedHlcCounts.Keys.GetEnumerator())
        {
            if (!enumerator.MoveNext())
            {
                return;
            }

            floor = enumerator.Current;
        }

        try
        {
            await cursorRegistry.ReportCursorAsync(
                _treeId,
                BlockedFloorConsumerId,
                HybridLogicalClock.Zero,
                floor,
                cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Buffer-rehydrate registry republish failed for tree {Tree}; "
                + "the next admit or removal will republish the pin through the applier.",
                _treeId);
        }
    }

    /// <summary>
    /// Cursor-registry consumer id under which both the applier's
    /// post-admit/remove report path and the buffer-grain's
    /// post-rehydration republish path publish the blocked-floor pin.
    /// Sharing one consumer id means a republish from activation is
    /// transparently superseded by the next applier report once the
    /// buffer state next changes — no stale pin lingers in the
    /// registry.
    /// </summary>
    internal const string BlockedFloorConsumerId = "applier:atomic-batch";

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

        // Rehydrate the saga blacklist from the disjoint "x/" prefix.
        // The two prefixes are independent (bytes 'b' < 'x'), so the
        // two scans can run in either order and produce the same
        // post-load state. The blacklist row's value is a single-
        // byte sentinel — the row's existence under "x/{id}" is the
        // membership signal; no per-row deserialization is required.
        await foreach (var kvp in _store.EntriesAsync(
            startInclusive: BlacklistKeyPrefix,
            endExclusive: BlacklistKeyPrefixEnd,
            cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            var keyText = kvp.Key;
            if (keyText.Length != BlacklistKeyPrefix.Length + 32)
            {
                // Malformed row (truncated / wrong shape) - skip
                // rather than crash activation. Same defensive
                // discipline as the staged-entry deserialization
                // catch above.
                continue;
            }

            if (Guid.TryParseExact(keyText.AsSpan(BlacklistKeyPrefix.Length), "N", out var id)
                && id != Guid.Empty)
            {
                _blacklistedTransactions.Add(id);
            }
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

        // Range-delete entries (and any future op that emits with
        // HybridLogicalClock.Zero) must not be staged: the
        // producer-side WAL GC reads the buffer's lowest staged HLC
        // and treats Zero as "no pin" (Zero >= positiveFloor is
        // always false). A staged Zero entry would silently disable
        // the GC pin for that tree, allowing the producer to trim
        // entries the receiver still needs to recover from buffer
        // state. The atomic-batch contract today only stamps Set /
        // Delete (which carry positive HLCs); this guard is the
        // forward-compatibility belt for that invariant.
        if (entry.Timestamp <= HybridLogicalClock.Zero)
        {
            throw new ArgumentException(
                "ReplogEntry.Timestamp must be strictly greater than HybridLogicalClock.Zero "
                + "for atomic-batch staging admission. Entries with Zero HLCs would silently "
                + "disable the producer-side blocked-floor GC pin.",
                nameof(entry));
        }

        // Blacklist short-circuit: a saga that the producer-side
        // quiesce path could not drain in time has its transaction
        // id registered here by the receiver's bootstrap state
        // machine. Any incremental entry carrying a blacklisted id
        // must NOT be staged — the missing siblings (committed
        // before the snapshot's AsOfHlc and already applied via the
        // snapshot drain) will never arrive on the incremental
        // stream, so the buffer would never reach completeness and
        // the orphan-timeout sweep would eventually evict the
        // partial batch. Bypassing the buffer instead delivers each
        // remaining key as a point write — degraded to causal+
        // atomic visibility for the blacklisted saga, no orphan
        // stuck. The bypass is observable to the caller via the
        // BlacklistedBypass flag on the admission result; the
        // ReplicationApplier branches on the flag and routes the
        // entry through the canonical point-apply path.
        //
        // The check happens after the per-entry guard rails so a
        // malformed entry still fails fast with the same exception
        // shape as the non-blacklisted path. The check happens
        // BEFORE the dedupe / capacity / persistence steps because
        // none of those side effects make sense for an entry the
        // caller is expected to apply directly.
        if (_blacklistedTransactions.Contains(entry.TransactionId))
        {
            return new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            };
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

    /// <inheritdoc />
    public Task<HybridLogicalClock?> GetLowestStagedHlcAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        if (_stagedHlcCounts.Count == 0)
        {
            return Task.FromResult<HybridLogicalClock?>(null);
        }

        // O(log N) lookup: the sorted multiset is maintained
        // incrementally by AdmitInMemory / RemoveTransactionAsync so
        // the smallest key is always the minimum staged HLC across
        // every partially-buffered transaction. Single grain-turn
        // visibility means the result is consistent with the most
        // recent admit / removal call against this activation.
        // SortedDictionary's first key is its minimum (the underlying
        // red-black tree's leftmost node).
        using var enumerator = _stagedHlcCounts.Keys.GetEnumerator();
        enumerator.MoveNext();
        return Task.FromResult<HybridLogicalClock?>(enumerator.Current);
    }

    /// <inheritdoc />
    public async Task RegisterBlacklistedTransactionsAsync(
        IReadOnlyList<Guid> transactionIds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transactionIds);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        if (transactionIds.Count == 0)
        {
            // No-op: an empty list is the steady-state happy path
            // (the producer-side quiesce drained every in-flight
            // saga before the snapshot scan began). Avoiding the
            // for-loop saves the small constant overhead of the
            // enumerator on the bootstrap hot path.
            return;
        }

        // Two-pass: validate the entire list first so a malformed
        // id later in the list does not leave the store in a
        // half-persisted shape with the prior ids written but the
        // in-memory set unmodified. Cumulative union: caller may
        // invoke this multiple times during a single bootstrap
        // (e.g. one call on transition to IncrementalHandoff plus
        // an idempotent retry call from a recovery path). Empty
        // Guids are rejected loudly so a malformed blacklist
        // surfaces as a caller error rather than silently widening
        // the bypass set with a sentinel id that cannot match any
        // producer-stamped TransactionId.
        for (var i = 0; i < transactionIds.Count; i++)
        {
            if (transactionIds[i] == Guid.Empty)
            {
                throw new ArgumentException(
                    $"transactionIds[{i}] must be non-empty.",
                    nameof(transactionIds));
            }
        }

        // Persist each new id to the system tree under the
        // disjoint "x/" prefix. Cancellation is honoured BETWEEN
        // per-id persists (a cancellation mid-call leaves the
        // already-persisted ids durable; the in-memory set is
        // only updated post-persist so the activation rehydration
        // and the live in-memory state remain consistent — a
        // post-cancellation reactivation will load the ids that
        // were persisted before the cancellation fired). Already-
        // known ids skip the round-trip.
        for (var i = 0; i < transactionIds.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var id = transactionIds[i];
            if (_blacklistedTransactions.Contains(id))
            {
                continue;
            }

            await _store.SetAsync(BlacklistKey(id), BlacklistRowValue, cancellationToken)
                .ConfigureAwait(true);
            _blacklistedTransactions.Add(id);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphansAsync(TimeSpan orphanTimeout, CancellationToken cancellationToken)
    {
        if (orphanTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(orphanTimeout),
                orphanTimeout,
                "Orphan timeout must be strictly greater than TimeSpan.Zero.");
        }
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        if (_admissionOrder.Count == 0)
        {
            return 0;
        }

        var cutoffTicks = DateTime.UtcNow.Ticks - orphanTimeout.Ticks;

        // Collect orphan keys before mutating the index. _admissionOrder
        // is FIFO by first-admit time, so we can stop at the first
        // non-orphan: every later transaction is younger by construction.
        // We use the per-transaction min(EnqueuedAtTicks) as the orphan
        // age clock so a transaction whose first sibling arrived long
        // ago but whose later siblings arrived recently is still
        // recognised as an orphan (the missing sibling is the slow one).
        //
        // Pre-sized to _admissionOrder.Count: in the common case where
        // the entire admission queue has aged past cutoff (heavy ack
        // loss, producer crash) we avoid the default-capacity-4 List
        // grow-and-copy cycle that would allocate O(log N) intermediate
        // arrays.
        var orphanKeys = new List<TransactionKey>(_admissionOrder.Count);
        foreach (var key in _admissionOrder)
        {
            if (!_byTransaction.TryGetValue(key, out var siblings) || siblings.Count == 0)
            {
                // Defensive self-heal: if invariant is violated
                // (admission order tracks a key that has no sibling
                // map row, or whose row was concurrently emptied),
                // continue scanning. The key is removed from
                // _admissionOrder + _admissionNodes inside the per-
                // orphan loop below where mutation under iteration
                // is safe.
                continue;
            }

            var oldestStagedTicks = long.MaxValue;
            foreach (var staged in siblings.Values)
            {
                if (staged.EnqueuedAtTicks < oldestStagedTicks)
                {
                    oldestStagedTicks = staged.EnqueuedAtTicks;
                }
            }

            if (oldestStagedTicks > cutoffTicks)
            {
                // FIFO ordering: every subsequent transaction was
                // first-admitted no earlier than this one (admission
                // append-at-end keeps _admissionOrder monotonic in
                // first-admit time). Stop scanning.
                break;
            }

            orphanKeys.Add(key);
        }

        if (orphanKeys.Count == 0)
        {
            return 0;
        }

        var hwm = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(_treeId);
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeId);
        var floorChanged = false;

        // Build the failure-reason string ONCE per sweep. The orphan
        // timeout is constant within a single SweepOrphansAsync call,
        // so per-displaced interpolation would allocate one heap-string
        // per evicted entry — wasteful when an orphan with N siblings
        // is the common shape (a 100-entry orphan produces 99 redundant
        // copies of the same string).
        var failureReason =
            "Orphaned atomic-batch transaction: stuck in the per-tree staging buffer "
            + $"longer than the configured orphan timeout ({orphanTimeout}).";
        var evicted = 0;

        foreach (var key in orphanKeys)
        {
            // Cancellation honoured strictly BETWEEN orphans, not
            // mid-orphan. Once we begin DLQ-parking an orphan's
            // entries, we commit to running the full
            // (DLQ-park → Remove → HWM-advance) sequence so the orphan
            // never lands in a partial state where some-but-not-all
            // entries have been parked or where Remove ran without
            // any park attempt. Inner-loop cancellation is swallowed
            // for the same reason (see DLQ inner-catch below).
            cancellationToken.ThrowIfCancellationRequested();

            if (!_byTransaction.TryGetValue(key, out var siblings) || siblings.Count == 0)
            {
                // Defensive self-heal for the invariant-violation
                // path: drop the stale admission entry so subsequent
                // sweeps don't re-scan it forever. Mirrors the
                // detection-loop branch above; mutation here is safe
                // because we're iterating a snapshot list, not the
                // live _admissionOrder.
                if (_admissionNodes.Remove(key, out var staleNode))
                {
                    _admissionOrder.Remove(staleNode);
                }
                continue;
            }

            // (1) Snapshot displaced entries before we mutate the
            // in-memory index so the DLQ-park, Remove, and HWM-advance
            // steps see a stable view independent of subsequent
            // removals.
            var displaced = siblings.Values.ToArray();

            // (2) Compute the orphan's max HLC for the per-origin
            // high-water-mark advance. Range deletes carry
            // HybridLogicalClock.Zero, but admission rejects Zero so
            // every staged entry has a positive HLC by construction.
            var maxHlc = displaced[0].Entry.Timestamp;
            for (var i = 1; i < displaced.Length; i++)
            {
                if (displaced[i].Entry.Timestamp.CompareTo(maxHlc) > 0)
                {
                    maxHlc = displaced[i].Entry.Timestamp;
                }
            }

            // (3) Park every snapshot entry on the per-tree DLQ tagged
            // ReasonOrphanTransaction *first*, before the irreversible
            // Remove in step (4). Critical durability ordering: if the
            // DLQ throws after Remove has already deleted the system-
            // tree rows, the orphan is silently lost — the per-origin
            // HWM advance in step (5) would cause every future re-ship
            // from the producer to be filtered as a duplicate, leaving
            // no recovery path. Parking first means a DLQ failure
            // leaves the entries in the buffer for the next sweep to
            // retry.
            //
            // Per-displaced try/catch swallows OCE: dlq.EnqueueAsync
            // is NOT idempotent (it assigns a fresh entry id per call
            // and creates a new system-tree row regardless of whether
            // the same (key, timestamp, origin) tuple has been parked
            // before), so propagating cancellation mid-loop would
            // cause the next sweep to create duplicate DLQ rows for
            // already-parked entries. Per-orphan atomicity (commit
            // once we begin) is the safer trade.
            foreach (var staged in displaced)
            {
                try
                {
                    await dlq.EnqueueAsync(
                        staged.Entry,
                        failureReason,
                        retryCount: 0,
                        reasonTag: LatticeReplicationMetrics.ReasonOrphanTransaction,
                        cancellationToken).ConfigureAwait(true);
                }
                catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
                {
                    // Swallow rather than propagate: see comment above.
                    // The WAL still holds the original entry; the next
                    // sweep will retry parking and Remove has not yet
                    // run, so the orphan is fully recoverable.
                    _logger.LogWarning(
                        ex,
                        "Orphan-sweep DLQ enqueue cancelled mid-orphan for tree {Tree} entry key {Key}; "
                        + "the staged entry remains in the buffer and the next sweep will retry.",
                        _treeId, staged.Entry.Key);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Orphan-sweep DLQ enqueue failed for tree {Tree} entry key {Key}; "
                        + "the WAL still holds the original and the operator can recover via re-ship.",
                        _treeId, staged.Entry.Key);
                }
            }

            // (4) Remove from in-memory + system tree. This decrements
            // _stagedHlcCounts for every released entry, so the buffer's
            // blocked-floor pin advances naturally on the next
            // GetLowestStagedHlcAsync read. Irreversible — runs only
            // after the DLQ park attempts above have completed.
            await RemoveTransactionAsync(key, deleteFromStore: true, cancellationToken).ConfigureAwait(true);

            // Atomic-batch terminal-disposition counter: mirror the
            // eviction path's contract — every orphan that reaches the
            // dlq_orphan terminal disposition increments
            // <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
            // exactly once. The increment lives between the in-memory
            // remove and the HWM advance step so a sweep that is
            // cancelled mid-orphan (which we do not allow per the
            // commit-once-we-begin discipline above) cannot
            // double-record. Counter increment failures cannot occur
            // — UpDownCounter.Add and Counter.Add are infallible — so
            // there is no enclosing try/catch.
            LatticeReplicationMetrics.ApplyTxCompleted.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
                new KeyValuePair<string, object?>(
                    LatticeReplicationMetrics.TagOutcome,
                    LatticeReplicationMetrics.OutcomeTxDlqOrphan));

            floorChanged = true;
            evicted++;

            // (5) Advance the per-origin HWM past the orphan's max
            // HLC so causal-stream progress resumes. Best-effort: a
            // concurrent advance from a newer apply may already have
            // moved the HWM past this orphan, in which case
            // TryAdvanceAsync returns false and the call is a no-op.
            // OCE propagates here because the orphan has already been
            // DLQ-parked and Removed; an aborted HWM advance only
            // delays causal-stream progress for this origin until the
            // next inbound apply, which is the same recovery path the
            // generic-catch warning describes.
            try
            {
                await hwm.TryAdvanceAsync(key.OriginClusterId, maxHlc, cancellationToken)
                    .ConfigureAwait(true);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Orphan-sweep HWM advance failed for tree {Tree} origin {Origin} max-hlc {MaxHlc}; "
                    + "the next inbound apply against this origin will retry the advance.",
                    _treeId, key.OriginClusterId, maxHlc);
            }
        }

        // (6) Republish the buffer's blocked-floor pin so the
        // producer-side WAL garbage collector unpins through the
        // orphan's HLC window. The per-tree maintenance grain runs
        // this sweep outside the applier hot path, so without an
        // explicit republish the registry would still hold the
        // pre-sweep pin until the next applier admit / release
        // fires — which could delay the next GC pass by a full
        // maintenance cadence.
        //
        // Republish is best-effort: a registry failure here does not
        // unwind the eviction count returned to the caller, because
        // the eviction itself succeeded and the producer-side GC pin
        // will be republished by the next applier admit/release on
        // this tree.
        if (floorChanged)
        {
            try
            {
                await RepublishBlockedFloorAsync(cancellationToken).ConfigureAwait(true);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Orphan-sweep blocked-floor republish failed for tree {Tree}; "
                    + "the next applier admit / release on this tree will republish the pin.",
                    _treeId);
            }
        }

        return evicted;
    }

    /// <summary>
    /// Republishes the buffer's current blocked-floor state to
    /// <see cref="cursorRegistry"/> after a state change driven by
    /// the buffer grain itself (rehydration, orphan sweep) rather
    /// than by an applier admit/release. The applier owns the steady-
    /// state publish path; this helper covers the gaps where a
    /// state mutation happens without an applier in the loop.
    /// <para>
    /// When the buffer is empty after the mutation the registry pin
    /// is cleared via <see cref="ILatticeReplicationCursorRegistry.UnregisterAsync(string, string, CancellationToken)"/>;
    /// when the buffer still holds entries the lowest staged HLC is
    /// re-published with cursor <see cref="HybridLogicalClock.Zero"/>
    /// (the registry's "no cursor contributed" sentinel for buffer-
    /// only consumers, accepted by the blocked-floor overload). Both
    /// paths swallow registry failures: the applier's standard
    /// reporting path will republish on the next admit / release.
    /// </para>
    /// </summary>
    private async Task RepublishBlockedFloorAsync(CancellationToken cancellationToken)
    {
        if (cursorRegistry is null)
        {
            return;
        }

        try
        {
            if (_stagedHlcCounts.Count == 0)
            {
                await cursorRegistry.UnregisterAsync(
                    _treeId,
                    BlockedFloorConsumerId,
                    cancellationToken).ConfigureAwait(true);
                return;
            }

            HybridLogicalClock floor;
            using (var enumerator = _stagedHlcCounts.Keys.GetEnumerator())
            {
                enumerator.MoveNext();
                floor = enumerator.Current;
            }

            await cursorRegistry.ReportCursorAsync(
                _treeId,
                BlockedFloorConsumerId,
                HybridLogicalClock.Zero,
                floor,
                cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Buffer blocked-floor republish failed for tree {Tree} after a self-driven state change; "
                + "the next applier admit / release will republish the pin through the standard path.",
                _treeId);
        }
    }

    private void AdmitInMemory(TxStagedEntry staged, bool isRehydration)
    {
        var key = new TransactionKey(staged.OriginClusterId, staged.TransactionId);
        var newTransaction = false;
        if (!_byTransaction.TryGetValue(key, out var siblings))
        {
            siblings = new Dictionary<int, TxStagedEntry>();
            _byTransaction[key] = siblings;
            var node = _admissionOrder.AddLast(key);
            _admissionNodes[key] = node;
            newTransaction = true;
        }

        if (siblings.ContainsKey(staged.BatchIndex))
        {
            // Defensive on rehydration: a duplicated row in the
            // backing store (should not occur in practice) is
            // tolerated as a no-op.
            return;
        }

        siblings[staged.BatchIndex] = staged;
        var addedBytes = EstimateBytes(staged);
        _trackedBytes += addedBytes;
        IncrementStagedHlc(staged.Entry.Timestamp);

        // Atomic-batch buffered-count / buffer-bytes deltas:
        // publish on every admission. Activation rehydration is
        // intentionally skipped — a silo restart re-admits the
        // entries it inherited from the prior silo's persisted state,
        // and re-incrementing here would silently double-count
        // occupancy across activations. The counters are therefore a
        // delta surface relative to the activation's lifetime, which
        // matches the documented contract on
        // <see cref="LatticeReplicationMetrics.ApplyTxBuffered"/>.
        // Rehydrated keys are recorded in <see cref="_rehydratedKeys"/>
        // so the matching decrement in
        // <see cref="RemoveTransactionAsync(TransactionKey, bool, CancellationToken)"/>
        // can be suppressed symmetrically — without that bookkeeping
        // the gauge dips below the live-admission volume on the next
        // remove (apply / evict / sweep) of any rehydrated entry.
        if (isRehydration)
        {
            if (newTransaction)
            {
                _rehydratedKeys.Add(key);
            }
        }
        else
        {
            if (newTransaction)
            {
                LatticeReplicationMetrics.ApplyTxBuffered.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId));
            }

            LatticeReplicationMetrics.ApplyTxBufferBytes.Add(
                addedBytes,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId));
        }
    }

    /// <summary>
    /// Increments the reference count for <paramref name="hlc"/> in
    /// the sorted multiset that backs <see cref="GetLowestStagedHlcAsync(CancellationToken)"/>.
    /// </summary>
    private void IncrementStagedHlc(HybridLogicalClock hlc)
    {
        if (_stagedHlcCounts.TryGetValue(hlc, out var current))
        {
            _stagedHlcCounts[hlc] = current + 1;
        }
        else
        {
            _stagedHlcCounts[hlc] = 1;
        }
    }

    /// <summary>
    /// Decrements the reference count for <paramref name="hlc"/> in
    /// the sorted multiset; removes the key entirely when the count
    /// drops to zero.
    /// </summary>
    private void DecrementStagedHlc(HybridLogicalClock hlc)
    {
        if (!_stagedHlcCounts.TryGetValue(hlc, out var current))
        {
            // Defensive: a removal without a matching increment would
            // indicate a bookkeeping bug. Log-as-no-op rather than
            // throwing so an arithmetic regression does not crash the
            // grain activation; the next admit will re-anchor the
            // multiset.
            return;
        }

        if (current <= 1)
        {
            _stagedHlcCounts.Remove(hlc);
        }
        else
        {
            _stagedHlcCounts[hlc] = current - 1;
        }
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

        // Atomic-batch terminal-disposition counter: every transaction
        // that reaches a terminal disposition increments
        // <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
        // exactly once across one of the four outcome partitions. A
        // capacity eviction terminates the displaced transaction
        // before any apply attempt — its siblings are routed to the
        // DLQ tagged <see cref="LatticeReplicationMetrics.ReasonEvicted"/>
        // so an operator can recover them later if desired, but from
        // the visibility surface's perspective the transaction is
        // closed and the counter advances once for the whole batch.
        LatticeReplicationMetrics.ApplyTxCompleted.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId),
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagOutcome,
                LatticeReplicationMetrics.OutcomeTxEvictedCapacity));

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
            DecrementStagedHlc(staged.Entry.Timestamp);
        }
        _trackedBytes -= releasedBytes;
        if (_trackedBytes < 0)
        {
            // Defensive clamp: should never trip in production but
            // protects observability if an estimate drifts.
            _trackedBytes = 0;
        }

        // Mirror the per-tree atomic-batch buffered / buffer-bytes
        // deltas symmetrically with
        // <see cref="AdmitInMemory(TxStagedEntry, bool)"/>. Removal
        // is invoked from every terminal disposition path
        // (apply-on-completion, capacity eviction, orphan sweep),
        // so a single decrement here keeps the counters consistent
        // across every terminal-disposition outcome partition. Keys
        // admitted via activation rehydration intentionally suppress
        // both the admit-time increment *and* this decrement — the
        // gauge is "live admission lifecycle, not durable buffer
        // occupancy", so a rehydrated transaction that terminates
        // during this activation must not reduce the gauge below
        // the live-admission volume.
        var wasRehydrated = _rehydratedKeys.Remove(key);
        if (!wasRehydrated)
        {
            LatticeReplicationMetrics.ApplyTxBuffered.Add(
                -1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId));
            if (releasedBytes > 0)
            {
                LatticeReplicationMetrics.ApplyTxBufferBytes.Add(
                    -releasedBytes,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeId));
            }
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
