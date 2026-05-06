using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree atomic-batch staging buffer grain. Holds replog
/// entries that carry a non-zero
/// <see cref="ReplogEntry.AtomicBatchSize"/> until every sibling in
/// the enclosing <c>SetManyAtomicAsync</c> transaction has arrived,
/// then surfaces the completed batch to the caller so the receiver
/// can apply every key under one saga and preserve cross-cluster
/// atomic visibility.
/// <para>
/// Grain key format: the tree id verbatim. A replicated tree
/// therefore has at most one buffer activation regardless of how
/// many origin clusters publish to it; cross-origin batches are
/// keyed independently inside the buffer by the tuple
/// <c>(originClusterId, transactionId)</c>.
/// </para>
/// <para>
/// Storage is delegated to a reserved system tree
/// <c>_lattice_replog_txbuf_{treeId}</c> resolved through
/// <c>ISystemLattice</c>, so a silo crash mid-batch does not lose
/// staged entries. On activation the grain bulk-loads every staged
/// entry under its prefix and resumes admission from where it left
/// off.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationTxBufferGrain)]
internal interface IReplicationTxBufferGrain : IGrainWithStringKey
{
    /// <summary>
    /// Admits <paramref name="entry"/> to the buffer. The entry must
    /// carry a non-zero <see cref="ReplogEntry.AtomicBatchSize"/>;
    /// the caller is expected to branch on that field before calling
    /// this method (single-key non-atomic writes go through the
    /// existing point-apply path). Returns a result that signals
    /// whether the admission completed the enclosing batch and, when
    /// it did, the full batch in canonical
    /// <see cref="TxStagedEntry.BatchIndex"/> order.
    /// <para>
    /// Re-delivery of an entry whose
    /// <c>(originClusterId, transactionId, batchIndex)</c> identity
    /// tuple is already staged returns
    /// <see cref="TxBufferAdmissionResult.Deduped"/> set to
    /// <c>true</c> with no side effects. This is the wire-shape
    /// idempotency contract — a producer re-shipping the same entry
    /// after a transient receiver failure does not inflate the
    /// buffer.
    /// </para>
    /// <para>
    /// When the buffer reaches <c>AtomicBatchBufferMaxTransactions</c>
    /// or the cumulative payload exceeds
    /// <c>AtomicBatchBufferMaxBytes</c>, the oldest partially-buffered
    /// transaction (FIFO by enqueue time of its first entry) is
    /// evicted to make room. Eviction routes the displaced entries
    /// through the per-tree dead-letter queue tagged
    /// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>; the
    /// displaced batch is durably re-shippable because the producer's
    /// per-origin high-water-mark was never advanced past it.
    /// </para>
    /// </summary>
    Task<TxBufferAdmissionResult> AdmitAsync(ReplogEntry entry, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the number of distinct
    /// <c>(originClusterId, transactionId)</c> transactions currently
    /// partially buffered. Diagnostic helper used by tests and the
    /// observability surface.
    /// </summary>
    Task<int> CountTransactionsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the cumulative payload bytes the buffer is currently
    /// holding (sum over every staged entry's
    /// <see cref="ReplogEntry.Value"/> length plus a small per-entry
    /// overhead). Diagnostic helper.
    /// </summary>
    Task<long> CountBytesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the lowest <see cref="HybridLogicalClock"/>
    /// across every entry the buffer is currently holding, or
    /// <see langword="null"/> when the buffer is empty. The receiver-
    /// side applier publishes this value through the
    /// <see cref="ILatticeReplicationCursorRegistry"/> blocked-floor
    /// reporting overload after every admit / batch-completion event
    /// so the producer-side WAL garbage collector AND-s a strict-less
    /// <c>entry.Timestamp &lt; blockedFloor</c> clause into its trim
    /// predicate. Without this pin, the producer could trim a WAL
    /// entry that the receiver has staged-but-not-yet-applied — and
    /// a subsequent buffer loss (silo crash, eviction, orphan
    /// timeout) would leave the receiver unable to recover the
    /// missing key.
    /// <para>
    /// Computed as <c>min(staged.Entry.Timestamp)</c> across every
    /// staged entry of every partially-buffered transaction; the
    /// scan is O(N) in the total staged-entry count and is cheap
    /// because the buffer is typically empty or holds a handful of
    /// in-flight batches at a time.
    /// </para>
    /// </summary>
    Task<HybridLogicalClock?> GetLowestStagedHlcAsync(CancellationToken cancellationToken);
}
