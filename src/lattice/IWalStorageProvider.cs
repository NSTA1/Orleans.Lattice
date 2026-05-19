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
    /// Zero-copy overload of <see cref="AppendBatchAsync"/> that
    /// takes pre-encoded payload bytes alongside their parallel
    /// dense offsets. Producers (the WAL grain) call this overload
    /// when they have already paid the encode cost via
    /// <see cref="IWalMutationEncoder"/> at append time, so the bytes
    /// that informed the per-batch byte budget are the same bytes
    /// handed to the provider - no second encode.
    /// <para>
    /// Backends that natively store binary payloads (Azure Table
    /// Storage, file-backed providers) hand the segments straight
    /// through to their persistence row. Backends that prefer to own
    /// the codec (for example, ones that index secondary fields off
    /// <see cref="LatticeMutation.Timestamp"/> or
    /// <see cref="LatticeMutation.OriginClusterId"/>) decode each
    /// segment with the configured <see cref="IWalMutationEncoder"/>
    /// before storing.
    /// </para>
    /// <para>
    /// All-or-nothing atomicity, offset density, and overlap
    /// semantics are identical to <see cref="AppendBatchAsync"/>; the
    /// only difference is the input shape. The default implementation
    /// decodes the segments to <see cref="WalEntry"/> values and
    /// delegates to <see cref="AppendBatchAsync"/>, so a provider
    /// authored against the <see cref="WalEntry"/>-shaped contract
    /// continues to work without recompiling - implementations that
    /// can skip the round-trip override this method to gain the
    /// zero-copy fast path.
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="encodedEntries">Pre-encoded payload bytes for each entry, in the same order as <paramref name="offsets"/>. Segment lengths are arbitrary; segments are owned by the caller and must not be retained past the returned task's completion.</param>
    /// <param name="offsets">Dense ascending offsets parallel to <paramref name="encodedEntries"/>. Length must match.</param>
    /// <param name="encoder">Encoder used to decode segments for the default fallback implementation. Providers that override this method may ignore it.</param>
    /// <param name="cancellationToken">Cancellation token observed before the durable write commences.</param>
    Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalMutationEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        if (encodedEntries.Length != offsets.Length)
        {
            throw new ArgumentException(
                $"Encoded segment count ({encodedEntries.Length}) does not match offset count ({offsets.Length}); the two sequences must be parallel.",
                nameof(encodedEntries));
        }

        // Default fallback: decode each segment to a WalEntry and
        // delegate to the legacy overload so third-party providers
        // that have not implemented this method keep working.
        // Providers that can store the segments directly (e.g.
        // AzureTableWalStorageProvider) override this method and
        // skip the round-trip.
        var segments = encodedEntries.Span;
        var offsetSpan = offsets.Span;
        var decoded = new WalEntry[segments.Length];
        for (var i = 0; i < segments.Length; i++)
        {
            decoded[i] = new WalEntry
            {
                Offset = offsetSpan[i],
                Mutation = encoder.Decode(segments[i].AsSpan()),
            };
        }
        return AppendBatchAsync(treeId, shardIndex, decoded, cancellationToken);
    }

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

    /// <summary>
    /// Activation-time recovery hook. Called by the WAL grain's
    /// <c>OnActivateAsync</c> immediately after
    /// <see cref="GetHighestOffsetAsync"/>, before the grain accepts
    /// any appends, so the backend can reconcile any state that a
    /// crash between transactional phases may have left in a
    /// half-committed form. The method must complete (success or
    /// throw) before the grain is observable to callers.
    /// <para>
    /// The contract is "leave the log in a state where
    /// <see cref="GetHighestOffsetAsync"/>,
    /// <see cref="GetLowestOffsetAsync"/>, and
    /// <see cref="ReadAsync"/> all agree on the persisted tail" - the
    /// reconciliation step is permitted to either roll forward (commit
    /// missing manifest rows for fully-written batch partitions) or
    /// roll back (delete the orphan batch partitions), at the
    /// implementation's discretion.
    /// </para>
    /// <para>
    /// The default implementation is a no-op: single-transaction
    /// backends (the in-memory provider, any provider whose commit
    /// path is a single atomic operation) have no orphan state to
    /// reconcile. Multi-phase backends (the Azure Table Storage
    /// provider's per-batch partition + manifest layout) override
    /// this method to scan their commit log for orphans and either
    /// finish the commit or revert it.
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="cancellationToken">Cancellation token observed before any I/O commences.</param>
    Task ReconcileAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }
}
