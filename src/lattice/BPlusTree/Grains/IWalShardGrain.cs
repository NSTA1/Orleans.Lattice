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
