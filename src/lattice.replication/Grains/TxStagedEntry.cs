namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A single replog entry staged on the per-tree atomic-batch buffer
/// (see <see cref="IReplicationTxBufferGrain"/>). Carries the
/// originating <see cref="ReplogEntry"/> verbatim alongside the
/// composite key the buffer uses to detect sibling membership and
/// the index inside the enclosing transaction so an out-of-order
/// re-delivery of the same index is recognised as a duplicate
/// rather than as a sibling.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.TxStagedEntry)]
[Immutable]
internal readonly record struct TxStagedEntry
{
    /// <summary>The originating cluster id.</summary>
    [Id(0)] public string OriginClusterId { get; init; }

    /// <summary>The enclosing atomic transaction id.</summary>
    [Id(1)] public Guid TransactionId { get; init; }

    /// <summary>Total entries in the enclosing transaction (mirrors <see cref="ReplogEntry.AtomicBatchSize"/>).</summary>
    [Id(2)] public int BatchSize { get; init; }

    /// <summary>Zero-based index of this entry within the transaction.</summary>
    [Id(3)] public int BatchIndex { get; init; }

    /// <summary>The replog entry being staged.</summary>
    [Id(4)] public ReplogEntry Entry { get; init; }

    /// <summary>Wall-clock tick at which the entry was admitted to the buffer.</summary>
    [Id(5)] public long EnqueuedAtTicks { get; init; }
}

/// <summary>
/// Result of a single admission call to
/// <see cref="IReplicationTxBufferGrain.AdmitAsync"/>. Communicates
/// whether the admission caused the enclosing batch to reach
/// completeness so the receiver can branch into the
/// hand-off-to-apply path. The completed entries are returned in
/// canonical <see cref="TxStagedEntry.BatchIndex"/> order so the
/// downstream apply path observes a deterministic key sequence
/// regardless of the wire-side delivery order.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.TxBufferAdmissionResult)]
[Immutable]
internal readonly record struct TxBufferAdmissionResult
{
    /// <summary>
    /// <c>true</c> when the admission completed the enclosing batch
    /// (i.e. the buffer now holds every index in
    /// <c>[0, BatchSize)</c>) and <see cref="CompletedBatch"/>
    /// carries the full batch in <see cref="TxStagedEntry.BatchIndex"/>
    /// order. <c>false</c> when the batch is still incomplete (or
    /// when the entry was deduped as a re-delivery of an existing
    /// index).
    /// </summary>
    [Id(0)] public bool BatchComplete { get; init; }

    /// <summary>
    /// <c>true</c> when the entry's
    /// <c>(originClusterId, transactionId, index)</c> identity tuple
    /// was already staged. The buffer treats this as a no-op
    /// admission — wire-shape duplicate re-deliveries pass through
    /// without inflating the buffer.
    /// </summary>
    [Id(1)] public bool Deduped { get; init; }

    /// <summary>
    /// The completed batch in canonical
    /// <see cref="TxStagedEntry.BatchIndex"/> order when
    /// <see cref="BatchComplete"/> is <c>true</c>. Empty otherwise.
    /// </summary>
    [Id(2)] public IReadOnlyList<TxStagedEntry> CompletedBatch { get; init; }
}
