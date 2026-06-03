using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-shard write-ahead-log grain. The single source of truth for
/// every captured <see cref="WalRecord"/> destined for both leaf
/// recovery (single-cluster durability) and downstream shippers (cross-cluster replication, when registered). Grains are keyed by
/// <c>{treeId}/{partition}</c> where <c>partition</c> is a stable hash
/// of the entry's key modulo <see cref="LatticeOptions.WalPartitions"/>.
/// <para>
/// Append is the commit point - the originating grain awaits
/// <see cref="AppendAsync"/> before its own write returns, so a WAL
/// failure surfaces to the original writer instead of being silently
/// swallowed in a best-effort post-write append.
/// </para>
/// <para>
/// Entries are assigned a monotonically-increasing per-shard sequence
/// number on append. Reads are by sequence cursor, mirroring the
/// at-least-once / advance-strictly-on-ack semantics later phases use
/// to drive the outbound shipper.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalShardGrain)]
internal interface IWalShardGrain : IGrainWithStringKey
{
    /// <summary>
    /// Persists <paramref name="entry"/> to the WAL and returns its
    /// assigned per-shard sequence number. Sequence numbers start at
    /// <c>0</c> and increase by one per append; gaps never appear in a
    /// successfully-persisted WAL.
    /// </summary>
    /// <param name="entry">The captured mutation record.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the originating call.</param>
    /// <returns>The sequence number assigned to the appended entry.</returns>
    Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken);

    /// <summary>
    /// Persists <paramref name="entries"/> to the WAL as a single
    /// grain-dispatch envelope and returns the per-shard sequence
    /// number assigned to each entry, in the same order as the input.
    /// Sequence numbers are dense and strictly ascending within the
    /// returned span (entry[i+1] == entry[i] + 1).
    /// <para>
    /// This is the batched counterpart to <see cref="AppendAsync"/>;
    /// the grain encodes every supplied <see cref="WalRecord"/> once,
    /// enqueues all encoded segments under a single state-gate hold,
    /// and parks every caller on a separate <see cref="TaskCompletionSource{TResult}"/>
    /// against a single in-flight flush. The whole batch shares one
    /// grain-dispatch turn (the caller pays one grain hop, not <c>N</c>);
    /// each entry's individual durability is signalled the same way
    /// <see cref="AppendAsync"/> signals it (per-entry TCS completion).
    /// </para>
    /// <para>
    /// All-or-nothing semantics are inherited from the underlying
    /// <see cref="IWalStorageProvider.AppendBatchAsync"/>: either every
    /// supplied entry is durably persisted before the returned task
    /// completes, or every entry's TCS faults with the same exception.
    /// The whole batch lands in a single per-batch flush window when
    /// the per-batch limits (<see cref="LatticeOptions.WalMaxBatchEntries"/>,
    /// <see cref="LatticeOptions.WalMaxBatchBytes"/>) allow it, and is
    /// transparently split across flushes by the same cutover protocol
    /// <see cref="AppendAsync"/> uses when the batch exceeds the limit.
    /// </para>
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> so multiple producer
    /// leaves / shard roots can enter the WAL grain concurrently and have
    /// their entries coalesce into the same in-flight flush window. Without
    /// this attribute the grain's default non-reentrant scheduling forces
    /// each caller to fully complete (assign offsets, kick a flush, await
    /// every per-entry TCS) before the next caller's turn starts, which
    /// pins the in-flight chain depth at 1 even when
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> is well above 1
    /// and starves the per-batch packer of concurrent entries. All
    /// mutable state inside the implementation is serialised by an
    /// internal state gate (offset assignment, pending-list mutation,
    /// in-flight cap enforcement), so concurrent interleaved turns are
    /// safe by construction and the dense / strictly-ascending offset
    /// invariant inside a single returned batch is preserved by the
    /// per-iteration gate hold.
    /// </para>
    /// </summary>
    /// <param name="entries">The captured mutation records to append, in caller-defined order. The returned offsets are parallel to this list.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the originating call.</param>
    /// <returns>An immutable list of per-shard sequence numbers, one per input entry, in input order.</returns>
    [AlwaysInterleave]
    Task<IReadOnlyList<long>> AppendBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken);

    /// <summary>
    /// Returns up to <paramref name="maxEntries"/> entries with sequence
    /// number greater than or equal to <paramref name="fromSequence"/>,
    /// in ascending sequence order. The returned page carries the
    /// sequence number to use on the next call as
    /// <see cref="WalShardPage.NextSequence"/>.
    /// </summary>
    /// <param name="fromSequence">Inclusive starting sequence number.</param>
    /// <param name="maxEntries">Maximum number of entries to return; must be at least 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<WalShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken);

    /// <summary>
    /// Bytes-shaped counterpart to <see cref="ReadAsync"/>. Returns the
    /// same per-shard window of entries strictly greater than or equal
    /// to <paramref name="fromSequence"/>, ascending, up to
    /// <paramref name="maxEntries"/>, but each entry carries the
    /// pre-encoded payload bytes the canonical
    /// <see cref="IWalRecordEncoder"/> wrote at append time instead of
    /// the materialised <see cref="WalRecord"/>. The replication
    /// shipper drains this method to feed the framing-only outbound
    /// transport seam without paying a per-send Orleans envelope
    /// serialize - the encode happens once at append time and the
    /// bytes are reused verbatim on every send to every peer.
    /// <para>
    /// Implementations resolve the encoded bytes from the underlying
    /// <see cref="IWalStorageProvider.ReadEncodedAsync"/> seam, which
    /// providers that natively store encoded bytes (the Azure Table
    /// provider) override to return rows verbatim.
    /// </para>
    /// </summary>
    /// <param name="fromSequence">Inclusive starting sequence number.</param>
    /// <param name="maxEntries">Maximum number of entries to return; must be at least 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<WalShardShippingPage> ReadShippingAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the next sequence number that will be assigned by the
    /// next successful <see cref="AppendAsync"/>. Equal to the number
    /// of entries currently persisted. <c>0</c> when the WAL is empty.
    /// </summary>
    Task<long> GetNextSequenceAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the number of <i>live</i> entries currently persisted in
    /// this WAL shard - i.e. the count of entries between the lowest
    /// still-stored offset and the highest assigned offset, inclusive.
    /// For an untrimmed shard this equals the next sequence number
    /// (the canonical "how many entries have I written" answer). After
    /// <see cref="IWalStorageProvider.TrimAsync"/> removes a prefix
    /// (driven by <see cref="ILatticeWalGc"/> once every consumer has
    /// acked past that point) the live count drops by exactly the
    /// trimmed prefix length, so dashboards, alerts, and the
    /// back-pressure health check observe the persisted footprint
    /// rather than a monotonically-growing offset counter.
    /// </summary>
    Task<long> GetLiveEntryCountAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the approximate number of retained on-wire payload bytes
    /// currently persisted in this WAL shard, or <c>-1</c> when the
    /// configured <see cref="IWalStorageProvider"/> does not support byte
    /// accounting. Trim-aware: <see cref="IWalStorageProvider.TrimAsync"/>
    /// reduces the figure as a prefix is removed. The byte-accurate
    /// storage-usage aggregator (<see cref="ILattice.GetStorageUsageAsync"/>)
    /// sums this across a tree's WAL shards to report the tree's retained
    /// WAL footprint. Forwards directly to
    /// <see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> so a storage-usage
    /// poll never queues behind an in-flight <c>AppendBatchAsync</c> /
    /// <c>TrimAsync</c> turn. The call is a read-only manifest scan on
    /// the underlying provider; it touches no shared in-grain state and
    /// safely interleaves with concurrent appends and trims (the
    /// returned figure is an at-a-moment snapshot, bounded over-report
    /// by one batch's worth of slack across a concurrent trim, exactly
    /// the contract callers already expect).
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [AlwaysInterleave]
    Task<long> GetRetainedByteSizeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Diagnostic helper: returns the number of entries currently
    /// persisted in this WAL shard. <b>Trim-unaware</b> -
    /// <see cref="IWalStorageProvider.TrimAsync"/> reduces the
    /// persisted footprint without updating this counter, so callers
    /// that want the live footprint (dashboards, alerts, back-pressure)
    /// must use <see cref="GetLiveEntryCountAsync"/> instead. Retained
    /// as an obsolete forwarder for one minor version so existing
    /// callers compile without immediate change.
    /// </summary>
    [Obsolete("Use GetLiveEntryCountAsync instead. GetEntryCountAsync is not trim-aware and will be removed in a future minor version.", DiagnosticId = "LATTICE0001")]
    Task<long> GetEntryCountAsync(CancellationToken cancellationToken);
}
