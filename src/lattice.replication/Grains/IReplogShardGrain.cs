namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-shard write-ahead-log grain. The single source of truth for
/// every captured <see cref="ReplogEntry"/> destined for downstream
/// shippers. Grains are keyed by <c>{treeId}/{partition}</c> where
/// <c>partition</c> is a stable hash of the entry's key modulo
/// <see cref="LatticeReplicationOptions.ReplogPartitions"/>.
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
[Alias(ReplicationTypeAliases.IReplogShardGrain)]
internal interface IReplogShardGrain : IGrainWithStringKey
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
    Task<long> AppendAsync(ReplogEntry entry, CancellationToken cancellationToken);

    /// <summary>
    /// Returns up to <paramref name="maxEntries"/> entries with sequence
    /// number greater than or equal to <paramref name="fromSequence"/>,
    /// in ascending sequence order. The returned page carries the
    /// sequence number to use on the next call as
    /// <see cref="ReplogShardPage.NextSequence"/>.
    /// </summary>
    /// <param name="fromSequence">Inclusive starting sequence number.</param>
    /// <param name="maxEntries">Maximum number of entries to return; must be at least 1.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ReplogShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the next sequence number that will be assigned by the
    /// next successful <see cref="AppendAsync"/>. Equal to the number
    /// of entries currently persisted. <c>0</c> when the WAL is empty.
    /// </summary>
    Task<long> GetNextSequenceAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the number of entries currently persisted in this WAL
    /// shard. Diagnostic helper for tests and metrics.
    /// </summary>
    Task<long> GetEntryCountAsync(CancellationToken cancellationToken);
}
