namespace Orleans.Lattice;

/// <summary>
/// Pluggable durability seam for the per-shard write-ahead log. Lets a
/// host swap the WAL's underlying storage backend (Orleans grain
/// persistence, Azure Table Storage, an in-memory test fake) without
/// touching the rest of the commit-log pipeline. Registered at silo
/// startup via <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/>;
/// the replication package additionally exposes per-tree configurability
/// via <c>LatticeReplicationOptions.WalStorageProvider</c>.
/// <para>
/// <b>Atomicity contract.</b> <see cref="AppendBatchAsync"/> is
/// all-or-nothing per call: either every entry in the supplied list is
/// durably persisted before the returned task completes, or none of
/// them are. Backends that cannot meet that contract for a particular
/// batch (for example, a multi-partition write on a backend that does
/// not offer cross-partition transactions) must reject the batch at
/// validation time rather than silently fragmenting it.
/// </para>
/// <para>
/// <b>Offset density.</b> Offsets supplied in <see cref="WalEntry.Offset"/>
/// are caller-assigned. Within a single <see cref="AppendBatchAsync"/>
/// call they are strictly ascending and gap-free (entry[i+1].Offset ==
/// entry[i].Offset + 1). Across calls they are dense in aggregate under
/// normal operation - the WAL grain assigns offsets monotonically under
/// the grain turn - but with
/// <see cref="LatticeOptions.WalMaxPendingBatches"/> > 1 a single shard
/// can issue multiple concurrent <see cref="AppendBatchAsync"/> calls
/// whose batches may arrive at the provider out of order, and a failed
/// flush may leave a permanent gap in the log (downstream consumers
/// observe the gap honestly). Implementations must preserve the
/// supplied offsets verbatim and must reject overlap with any already-
/// persisted offset; they must not assume contiguity with the persisted
/// tail.
/// </para>
/// <para>
/// <b>Cross-package consumer.</b> The contract is identical between
/// today's replication-only WAL consumer and the future log-first
/// commit-point model in which the WAL is the sole durability mechanism
/// - see <c>docs/future.md</c>. Implementations authored against this
/// interface today are reusable in v2 without API change.
/// </para>
/// </summary>
public interface IWalStorageProvider
{
    /// <summary>
    /// Atomically appends <paramref name="entries"/> to the WAL for
    /// <paramref name="treeId"/> / <paramref name="shardIndex"/>. The
    /// task completes only after every supplied entry is durably
    /// persisted. On failure, no entry from the batch may remain
    /// observable to <see cref="ReadAsync"/> or
    /// <see cref="GetHighestOffsetAsync"/>.
    /// </summary>
    /// <param name="treeId">Logical tree id; identifies the WAL the batch belongs to. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="entries">Entries to append, in ascending <see cref="WalEntry.Offset"/> order. Offsets must be dense and equal to <c>currentHighest + 1, +2, …</c>; the implementation is permitted (but not required) to validate that.</param>
    /// <param name="cancellationToken">Cancellation token observed before the durable write commences.</param>
    Task AppendBatchAsync(
        string treeId,
        int shardIndex,
        IReadOnlyList<WalEntry> entries,
        CancellationToken cancellationToken);

    /// <summary>
    /// Yields entries with <see cref="WalEntry.Offset"/> strictly greater
    /// than <paramref name="fromOffsetExclusive"/>, in ascending offset
    /// order, up to a maximum of <paramref name="maxEntries"/>. The
    /// enumeration completes when either the limit is reached or the
    /// underlying log is exhausted.
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="fromOffsetExclusive">Strict lower-bound offset; pass <c>-1</c> to read from the start of the log.</param>
    /// <param name="maxEntries">Maximum number of entries to yield; must be at least <c>1</c>.</param>
    /// <param name="cancellationToken">Cancellation token observed between every yielded entry.</param>
    IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        CancellationToken cancellationToken);

    /// <summary>
    /// Returns the highest <see cref="WalEntry.Offset"/> currently
    /// persisted for <paramref name="treeId"/> /
    /// <paramref name="shardIndex"/>, or <c>-1</c> when the WAL is
    /// empty. Used by the WAL grain on activation to recover its
    /// next-offset counter without reading the whole log.
    /// </summary>
    Task<long> GetHighestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken);

    /// <summary>
    /// Returns the lowest <see cref="WalEntry.Offset"/> currently
    /// persisted for <paramref name="treeId"/> /
    /// <paramref name="shardIndex"/>, or <c>-1</c> when the WAL has no
    /// entries (either never written or fully trimmed). The default
    /// for an untrimmed shard is <c>0</c>; once
    /// <see cref="TrimAsync"/> has removed a prefix, this returns the
    /// first still-stored offset. Together with
    /// <see cref="GetHighestOffsetAsync"/> this lets a caller compute
    /// the number of live entries currently persisted as
    /// <c>highest - lowest + 1</c> without scanning the log; the WAL
    /// grain uses that pair to expose a trim-aware live entry count
    /// to diagnostics, dashboards, and back-pressure consumers.
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="cancellationToken">Cancellation token observed before the read.</param>
    Task<long> GetLowestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken);

    /// <summary>
    /// Trims every entry with offset less than or equal to
    /// <paramref name="throughOffsetInclusive"/> from the WAL. Called by
    /// the GC predicate (<see cref="ILatticeWalGc"/>) once every consumer
    /// has acked past that point. Idempotent - trimming through an offset that
    /// has already been trimmed is a no-op. Trimming through an offset
    /// that does not yet exist is permitted and reserves the trim point
    /// for a future append.
    /// </summary>
    Task TrimAsync(
        string treeId,
        int shardIndex,
        long throughOffsetInclusive,
        CancellationToken cancellationToken);
}
