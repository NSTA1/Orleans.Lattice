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
    /// <see cref="IWalRecordEncoder"/> at append time, so the bytes
    /// that informed the per-batch byte budget are the same bytes
    /// handed to the provider - no second encode.
    /// <para>
    /// Backends that natively store binary payloads (Azure Table
    /// Storage, file-backed providers) hand the segments straight
    /// through to their persistence row. Backends that prefer to own
    /// the codec (for example, ones that index secondary fields off
    /// <see cref="WalRecord.Timestamp"/> or
    /// <see cref="WalRecord.OriginClusterId"/>) decode each segment
    /// with the configured <see cref="IWalRecordEncoder"/> before
    /// storing.
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
        IWalRecordEncoder encoder,
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

        // Default fallback: decode each segment back to the durability-
        // shaped WalRecord, project it onto the provider-boundary
        // WalEntry.Mutation (LatticeMutation-shaped), and delegate to
        // the legacy overload so third-party providers that have not
        // implemented this method keep working. Providers that can
        // store the segments directly (e.g. AzureTableWalStorageProvider)
        // override this method and skip the round-trip.
        var segments = encodedEntries.Span;
        var offsetSpan = offsets.Span;
        var decoded = new WalEntry[segments.Length];
        for (var i = 0; i < segments.Length; i++)
        {
            // Re-stamp TreeId from the partition-key context: the
            // producer's Encode strips the slot from the encoded bytes
            // (every storage seam already supplies the tree id as a
            // method parameter, so persisting it on every entry is
            // pure duplication). The decode overload is the single
            // place that restoration happens.
            var record = encoder.Decode(segments[i].AsSpan(), treeId);
            decoded[i] = new WalEntry
            {
                Offset = offsetSpan[i],
                Mutation = BPlusTree.Grains.WalRecordConverter.FromWalRecord(in record),
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
    /// Bytes-shaped read seam mirroring
    /// <see cref="AppendEncodedBatchAsync"/>'s zero-copy write seam.
    /// Returns the same entries
    /// <see cref="ReadAsync"/> would yield - strictly greater than
    /// <paramref name="fromOffsetExclusive"/>, ascending, up to
    /// <paramref name="maxEntries"/> - but as pre-encoded byte
    /// segments rather than materialised <see cref="WalEntry"/>
    /// values. Used by the shipper drain to hand segments straight to
    /// the outbound framing encoder without an intermediate
    /// <see cref="WalRecord"/> materialisation.
    /// <para>
    /// The default implementation drains <see cref="ReadAsync"/> and
    /// re-encodes each entry's projected <see cref="WalRecord"/> via
    /// <paramref name="encoder"/>; providers that natively store the
    /// encoded bytes (the Azure Table Storage provider's row
    /// <c>Payload</c> column, an in-memory provider that retained the
    /// segments from <see cref="AppendEncodedBatchAsync"/>) override
    /// this method to return the bytes verbatim and skip the
    /// round-trip. Third-party providers that have not adopted the
    /// override continue to work unchanged - the default body
    /// preserves byte-for-byte equivalence with
    /// <see cref="ReadAsync"/> followed by an element-wise encode.
    /// </para>
    /// <para>
    /// The returned <see cref="WalShardEncodedPage"/> is transient:
    /// the underlying byte arrays are owned by the provider for the
    /// duration of the synchronous return and the caller must not
    /// retain references past consumption.
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="fromOffsetExclusive">Strict lower-bound offset; pass <c>-1</c> to read from the start of the log.</param>
    /// <param name="maxEntries">Maximum number of entries to yield; must be at least <c>1</c>.</param>
    /// <param name="encoder">Encoder used by the default fallback to project each <see cref="WalEntry.Mutation"/> back to a <see cref="WalRecord"/> and serialise it. Providers that override this method may ignore the argument. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the scan commences and between every yielded segment.</param>
    async Task<WalShardEncodedPage> ReadEncodedAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(encoder);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }
        cancellationToken.ThrowIfCancellationRequested();

        // Default fallback: drain ReadAsync and re-encode each entry's
        // projected WalRecord. This preserves byte-for-byte equivalence
        // with the zero-copy override path because (a) the producer
        // wrote bytes via IWalRecordEncoder.Encode(in WalRecord, writer)
        // through AppendEncodedBatchAsync, (b) the provider's read path
        // here materialises a WalEntry whose Mutation field round-trips
        // through WalRecordConverter, and (c) re-encoding the
        // converter-projected WalRecord through the same encoder
        // reproduces the original bytes for the subset of fields
        // LatticeMutation carries. Providers that hold the original
        // bytes (Azure Table Storage, or an in-memory provider with a
        // segment pool retained from AppendEncodedBatchAsync) override
        // this method and skip the round-trip.
        var segmentsBuilder = new List<ArraySegment<byte>>(Math.Min(maxEntries, 256));
        var offsetsBuilder = new List<long>(Math.Min(maxEntries, 256));
        await foreach (var entry in ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Pass the entry's durable merge mode through so the
            // re-encoded bytes are byte-faithful to the original append:
            // since wire id 26 the encoder persists Mode, so hardcoding
            // LwwRegister here would drop a CRDT record's mode from the
            // re-encoded payload. The mutation carries the authored mode
            // (recovered by the provider's decode), so it is the correct
            // source.
            var record = BPlusTree.Grains.WalRecordConverter.ToWalRecord(
                entry.Mutation,
                entry.Mutation.Mode,
                string.Empty);
            var writer = new System.Buffers.ArrayBufferWriter<byte>();
            encoder.Encode(in record, writer);
            segmentsBuilder.Add(new ArraySegment<byte>(writer.WrittenSpan.ToArray()));
            offsetsBuilder.Add(entry.Offset);
        }

        var segments = segmentsBuilder.ToArray();
        var offsets = offsetsBuilder.ToArray();
        return new WalShardEncodedPage
        {
            EncodedEntries = segments,
            Offsets = offsets,
            HighestOffsetInclusive = offsets.Length == 0 ? -1L : offsets[^1],
        };
    }

    /// <summary>
    /// Returns the highest <see cref="WalEntry.Offset"/> ever assigned for
    /// <paramref name="treeId"/> / <paramref name="shardIndex"/>, or <c>-1</c>
    /// when the shard has never accepted an entry. Used by the WAL grain on
    /// activation to recover its next-offset counter without reading the whole
    /// log.
    /// <para>
    /// <b>This is a monotonic high-water mark, not a live-entry maximum, and
    /// the distinction is load-bearing (issue #3366).</b> An implementation
    /// that answers from its live entries regresses to <c>-1</c> once a trim
    /// removes them all, so the grain restarts allocation at offset <c>0</c>
    /// while the shard's durable trim watermark is still far above it. Every
    /// entry appended after that point is born at or below the watermark, and
    /// the next recovery classifies it as already-trimmed and discards it -
    /// silently destroying acknowledged writes. A trim must therefore never
    /// lower this value: after trimming through offset <c>N</c> an empty shard
    /// must still answer <c>N</c>, so the next offset allocated is <c>N + 1</c>.
    /// </para>
    /// <para>
    /// <c>AzureTableWalStorageProvider</c> is the reference shape - it
    /// point-reads a dedicated persisted <c>TAIL</c> row that records the
    /// highest committed offset independently of which entry rows still exist,
    /// so a trim cannot drag it down.
    /// </para>
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
    /// that does not yet exist is permitted; whether the trim point is
    /// retained for a future append is provider-specific.
    /// </summary>
    Task TrimAsync(
        string treeId,
        int shardIndex,
        long throughOffsetInclusive,
        CancellationToken cancellationToken);

    /// <summary>
    /// Asks the provider to evaluate whether
    /// <paramref name="treeId"/> / <paramref name="shardIndex"/> is due for
    /// physical reclamation, and to perform it if its own policy says so.
    /// Called by the GC predicate (<see cref="ILatticeWalGc"/>) on a shard
    /// whose scan released nothing, so <see cref="TrimAsync"/> is not invoked
    /// for it on that pass.
    /// <para>
    /// This exists because for a log-structured backend the two are separable
    /// concerns that had been accidentally welded together. Trimming decides
    /// which <i>live</i> entries may be released, and is gated by consumers
    /// that have not yet applied them; reclamation returns space belonging to
    /// entries that are <i>already</i> trimmed and that nothing references. A
    /// provider that only reclaims as a side effect of being trimmed therefore
    /// cannot reclaim at all while the retention floor is held - and the floor
    /// being held is precisely the state in which the backlog is largest
    /// (issue #3207).
    /// </para>
    /// <para>
    /// The call carries no watermark and must not move one: it is not a trim,
    /// it grants no additional release, and it is safe to invoke on any shard
    /// at any time. Whether anything happens is entirely the provider's
    /// decision, taken against the same policy - and reported through the same
    /// instruments - as the evaluation <see cref="TrimAsync"/> performs. The
    /// contract is that the shard's <i>logical</i> contents are unchanged: the
    /// offsets <see cref="ReadAsync"/>, <see cref="GetLowestOffsetAsync"/> and
    /// <see cref="GetHighestOffsetAsync"/> report must be identical either
    /// side of the call.
    /// </para>
    /// <para>
    /// The default implementation is a no-op, which is correct and not an
    /// omission for a backend whose trim deletes its storage outright (the
    /// in-memory and Azure Table providers): deletion <i>is</i> reclamation
    /// there, so no dead bytes exist and there is nothing to evaluate. Only a
    /// backend that reclaims by rewriting - the file provider - has anything
    /// to do here.
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="cancellationToken">Cancellation token observed before any I/O commences.</param>
    Task EvaluateCompactionAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

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

    /// <summary>
    /// Returns the approximate number of retained on-wire payload bytes
    /// currently persisted for <paramref name="treeId"/> /
    /// <paramref name="shardIndex"/> - the summed encoded length of every
    /// live WAL entry between the lowest and highest still-stored offset
    /// (post-trim). Used by the byte-accurate storage-usage aggregator
    /// (<c>ILattice.GetStorageUsageAsync</c>) and the advisory
    /// byte-pressure WAL retention policy to report the tree's <b>logical</b>
    /// WAL size without scanning the log on the hot path.
    /// <para>
    /// The figure is the retained <b>payload</b> byte total: it counts the
    /// encoded mutation bytes a provider stores per entry and deliberately
    /// excludes backend-specific per-row framing overhead (partition keys,
    /// row keys, column names), which varies by provider and is not part
    /// of the logical WAL size. Providers may compute it from a running
    /// counter maintained at append/trim time (the recommended O(1) path)
    /// or by a bounded metadata read; they must not scan the full log on
    /// every call. A provider whose trim leaves a partially-trimmed
    /// boundary batch may over-report by at most one batch's payload, which
    /// is bounded and acceptable for the advisory uses above.
    /// </para>
    /// <para>
    /// <b>This figure does not bound physical disk usage, and must not be
    /// used to.</b> It excludes framing, and - the dominant term for a
    /// log-structured backend - it excludes <i>dead</i> bytes: payload
    /// already trimmed but not yet physically reclaimed. A provider that
    /// reclaims space by rewriting a segment (the file provider compacts
    /// only once dead bytes reach a configured fraction of payload) can
    /// legitimately hold as many dead bytes as live ones, so the retained
    /// total can understate real occupancy by a factor approaching two.
    /// Callers that need occupancy - a disk ceiling, a usage report - must
    /// use <see cref="GetPhysicalByteSizeAsync"/> and fall back to this
    /// figure only when the provider returns the unsupported sentinel.
    /// </para>
    /// <para>
    /// The default implementation returns <c>-1</c> to signal "byte
    /// accounting unsupported" - the aggregator renders the affected
    /// surface as partial rather than reporting a wrong zero. Providers
    /// that can account bytes cheaply override this method to return a
    /// non-negative total (<c>0</c> for an empty or fully-trimmed shard).
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="cancellationToken">Cancellation token observed before any read.</param>
    /// <returns>The retained payload byte total (>= 0), or <c>-1</c> when the provider does not support byte accounting.</returns>
    Task<long> GetRetainedByteSizeAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(-1L);
    }

    /// <summary>
    /// Returns the number of bytes this provider <b>physically occupies</b>
    /// for <paramref name="treeId"/> / <paramref name="shardIndex"/>: every
    /// byte the backend is holding on the shard's behalf, whether or not it
    /// is still logically live. This is the figure that bounds disk, and the
    /// one a capacity ceiling or a usage report must consult.
    /// <para>
    /// It differs from <see cref="GetRetainedByteSizeAsync"/> in two ways,
    /// both of which it <i>includes</i> where the retained total excludes
    /// them: per-record framing overhead, and <b>dead bytes</b> - payload
    /// that has been trimmed but not yet physically reclaimed. Dead bytes
    /// are not an anomaly to be smoothed over; for a log-structured backend
    /// that reclaims space by rewriting a segment, they are a designed-in
    /// component of occupancy whose steady-state size is set by the
    /// provider's compaction policy, and they can equal the live payload.
    /// </para>
    /// <para>
    /// The figure must be cheap: providers answer it from a running counter
    /// or a bounded metadata read (the file provider returns its write
    /// position, which is the literal file length), and must never scan the
    /// log. It is permitted to be an at-a-moment snapshot that a concurrent
    /// append or compaction immediately invalidates.
    /// </para>
    /// <para>
    /// The default implementation returns <c>-1</c> to signal "physical
    /// accounting unsupported", the same sentinel convention
    /// <see cref="GetRetainedByteSizeAsync"/> uses. Callers fall back to the
    /// retained total in that case, which is the right approximation for a
    /// backend whose trim deletes rows outright (the in-memory and Azure
    /// Table providers) and therefore carries no dead bytes at all.
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">Per-tree shard index.</param>
    /// <param name="cancellationToken">Cancellation token observed before any read.</param>
    /// <returns>The physical byte total (&gt;= 0), or <c>-1</c> when the provider does not support physical accounting.</returns>
    Task<long> GetPhysicalByteSizeAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(-1L);
    }
}
