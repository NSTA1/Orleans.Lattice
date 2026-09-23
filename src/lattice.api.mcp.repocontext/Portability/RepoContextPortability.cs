using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The single portability primitive over the repository-context store: a
/// namespace/prefix-scoped enumeration plus a versioned snapshot export and
/// import, built on the core Lattice cursor surface. It is the one reusable
/// mechanism the later backup/restore, external-index backfill, and
/// local-to-cloud re-seed flows compose from - none of which is built here.
/// <para>
/// <b>Enumeration</b> pages through every live entry under a key prefix in
/// ascending order using the core entry cursor, so TTL expiry and tombstone
/// hiding are honoured (a snapshot never carries a dead entry) and a scan is
/// resumable across process boundaries via an opaque continuation token.
/// </para>
/// <para>
/// <b>Export</b> serializes an enumerated range into a stable, provider-agnostic
/// snapshot stream (see <see cref="RepoContextSnapshotFormat"/>). <b>Import</b>
/// reads that stream back and, for each record, folds the incoming value into the
/// target store through a supplied CRDT <see cref="RepoContextSnapshotMerge"/>
/// strategy, so a re-import converges (no duplication) rather than overwriting.
/// </para>
/// <para>
/// <b>Expiry survives the round trip.</b> Export captures each entry's absolute
/// expiry and import reinstates it, so a time-to-live entry restores as one. The
/// distinction is load-bearing rather than cosmetic: an entry with a time-to-live
/// is one whose silent disappearance was judged acceptable, and a durable entry is
/// one whose loss would be a problem, so quietly promoting the first into the
/// second permanently resurrects records the store was entitled to shed. The
/// symmetric error is guarded too - a record whose expiry elapsed while the
/// snapshot sat at rest is dropped on restore rather than reinstated live.
/// </para>
/// <para>
/// The primitive is generic over the payload: the value bytes and the optional
/// vector / embedding-space tag are opaque, so it runs and is tested independently
/// of the concrete record and vector shapes those bytes decode to.
/// </para>
/// </summary>
internal static class RepoContextPortability
{
    /// <summary>The default number of records requested per enumeration page.</summary>
    internal const int DefaultPageSize = 256;

    /// <summary>
    /// Returns one page of the live entries under <paramref name="prefix"/> in
    /// ascending key order, resuming after <paramref name="continuationToken"/>
    /// when supplied. TTL-expired and tombstoned entries are never yielded.
    /// <para>
    /// Records come back with <see cref="RepoContextSnapshotRecord.ExpiresAtTicks"/>
    /// left at <c>0</c>. Enumeration is the package's general paging primitive and
    /// is read by a dozen retrieval paths, so it deliberately does not pay for the
    /// per-key versioned read that resolving an entry's expiry costs. Only
    /// <see cref="ExportAsync"/>, which must persist the expiry, opts into it.
    /// </para>
    /// </summary>
    /// <param name="tree">The Lattice tree to enumerate. Must not be <see langword="null"/>.</param>
    /// <param name="prefix">The key prefix that bounds the enumeration. Must not be <see langword="null"/>.</param>
    /// <param name="continuationToken">A token from a prior page to resume after, or <see langword="null"/> to start at the prefix.</param>
    /// <param name="pageSize">The maximum number of records to return. Must be positive.</param>
    /// <param name="vectorExport">An optional resolver for each record's opaque vector payload, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>A page of records with a continuation token and a has-more flag.</returns>
    internal static Task<RepoContextSnapshotPage> EnumerateAsync(
        ILattice tree,
        string prefix,
        string? continuationToken,
        int pageSize,
        RepoContextVectorExport? vectorExport,
        CancellationToken cancellationToken = default) =>
        EnumerateCoreAsync(
            tree, prefix, continuationToken, pageSize, vectorExport,
            captureExpiry: false, cancellationToken);

    private static async Task<RepoContextSnapshotPage> EnumerateCoreAsync(
        ILattice tree,
        string prefix,
        string? continuationToken,
        int pageSize,
        RepoContextVectorExport? vectorExport,
        bool captureExpiry,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var startInclusive = continuationToken is null ? prefix : Successor(continuationToken);
        var endExclusive = PrefixUpperBound(prefix);

        // Resilient page read: ScanEntriesAsync reopens over the still-live range
        // on a transient EnumerationAbortedException (silo failover, cold start,
        // idle expiry, scale-down) and resumes without gaps or duplicates. The
        // page's entries are buffered first so the underlying enumerator is
        // disposed before any per-record vector payload is resolved from another
        // tree, matching the original open/next/close-then-resolve ordering. One
        // extra entry beyond the page bound is probed to derive has-more directly
        // from the range rather than a conservative cursor flag.
        var raw = new List<KeyValuePair<string, byte[]>>(pageSize);
        var hasMore = false;
        await foreach (var entry in tree
            .ScanEntriesAsync(startInclusive, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (raw.Count == pageSize)
            {
                hasMore = true;
                break;
            }

            raw.Add(entry);
        }

        var records = new List<RepoContextSnapshotRecord>(raw.Count);
        string? lastKey = null;
        foreach (var entry in raw)
        {
            // Assigned before any skip below, so the continuation token still
            // advances past a key this page declined to emit.
            lastKey = entry.Key;

            var expiresAtTicks = 0L;
            if (captureExpiry)
            {
                // The entry cursor yields key and value only; an entry's expiry is
                // metadata that has to be read per key, and the core surface offers
                // no batched versioned read. This is the one cost export pays that
                // plain enumeration does not.
                var versioned = await tree
                    .GetWithVersionAsync(entry.Key, cancellationToken)
                    .ConfigureAwait(false);

                if (versioned is not null)
                {
                    if (versioned.Value is null)
                    {
                        // Expired or deleted between the scan and this read. A
                        // snapshot never carries a dead entry, so drop it.
                        continue;
                    }

                    expiresAtTicks = versioned.ExpiresAtTicks;
                }
            }

            RepoContextVectorPayload? vector = vectorExport is null
                ? null
                : await vectorExport(entry.Key, cancellationToken).ConfigureAwait(false);

            records.Add(new RepoContextSnapshotRecord
            {
                Key = entry.Key,
                Value = entry.Value,
                Vector = vector?.Vector,
                EmbeddingSpace = vector?.EmbeddingSpace,
                ExpiresAtTicks = expiresAtTicks,
            });
        }

        return new RepoContextSnapshotPage
        {
            Records = records,
            ContinuationToken = hasMore ? lastKey : null,
            HasMore = hasMore,
        };
    }

    /// <summary>
    /// Exports every live entry under <paramref name="prefix"/> to
    /// <paramref name="destination"/> as a versioned snapshot stream. Streams page
    /// by page, so an arbitrarily large range flows through without being fully
    /// buffered. Returns the number of records written.
    /// </summary>
    /// <param name="tree">The Lattice tree to export from. Must not be <see langword="null"/>.</param>
    /// <param name="prefix">The key prefix that bounds the export. Must not be <see langword="null"/>.</param>
    /// <param name="destination">The stream to write the snapshot to. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer for snapshot records. Must not be <see langword="null"/>.</param>
    /// <param name="vectorExport">An optional resolver for each record's opaque vector payload, or <see langword="null"/> for none.</param>
    /// <param name="pageSize">The enumeration page size. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the export.</param>
    /// <returns>The number of records written to the snapshot.</returns>
    internal static async Task<long> ExportAsync(
        ILattice tree,
        string prefix,
        Stream destination,
        Serializer serializer,
        RepoContextVectorExport? vectorExport = null,
        int pageSize = DefaultPageSize,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var writer = new RepoContextSnapshotWriter(destination, serializer);

        // Emit the header eagerly so an empty range still yields a valid snapshot.
        await writer.WriteHeaderAsync(cancellationToken).ConfigureAwait(false);

        long count = 0;
        string? token = null;
        while (true)
        {
            var page = await EnumerateCoreAsync(
                    tree, prefix, token, pageSize, vectorExport,
                    captureExpiry: true,
                    cancellationToken)
                .ConfigureAwait(false);

            foreach (var record in page.Records)
            {
                await writer.WriteRecordAsync(record, cancellationToken).ConfigureAwait(false);
                count++;
            }

            if (!page.HasMore)
            {
                break;
            }

            token = page.ContinuationToken;
        }

        return count;
    }

    /// <summary>
    /// Imports a snapshot stream produced by <see cref="ExportAsync"/> into
    /// <paramref name="tree"/>. Each record's value is folded into the store
    /// through <paramref name="merge"/> (defaulting to the record model's CRDT
    /// join), so the load is idempotent and a re-import converges without
    /// duplication. Any carried vector payload is applied through
    /// <paramref name="vectorImport"/> when supplied.
    /// <para>
    /// A record's captured expiry is reinstated rather than discarded, so a
    /// restored time-to-live entry stays one instead of being silently promoted to
    /// a durable record the store can never shed. Time the snapshot spent at rest
    /// counts against the entry's life, because the expiry is an absolute instant:
    /// a record whose expiry has already elapsed by restore time is
    /// <em>dropped</em>, not written, and counted in
    /// <see cref="RepoContextImportResult.RecordsExpired"/>. Writing it would
    /// resurrect an entry the source store was already entitled to shed, which is
    /// the same corruption in the opposite direction. Its vector payload is
    /// dropped with it, so no orphaned vector is left behind.
    /// </para>
    /// <para>
    /// When the key already exists in the target, the two expiries are joined the
    /// way the core value model joins them: a durable side wins (it is the weaker
    /// claim about disappearance, so honouring it can never shed an entry someone
    /// expects to keep), otherwise the later of the two instants wins. The
    /// reinstated life is expressed as a remaining duration at write time, so it
    /// can drift by the import's own latency - bounded, and in the safe direction
    /// of a marginally longer life rather than a premature disappearance.
    /// </para>
    /// </summary>
    /// <param name="tree">The Lattice tree to import into. Must not be <see langword="null"/>.</param>
    /// <param name="source">The snapshot stream to read. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer for snapshot records. Must not be <see langword="null"/>.</param>
    /// <param name="merge">The CRDT merge strategy, or <see langword="null"/> to use the record model's default.</param>
    /// <param name="vectorImport">An optional sink for each record's opaque vector payload, or <see langword="null"/> to ignore vectors.</param>
    /// <param name="timeProvider">The clock used to decide whether a captured expiry has already elapsed, or <see langword="null"/> for <see cref="TimeProvider.System"/>.</param>
    /// <param name="cancellationToken">Cancels the import.</param>
    /// <returns>The import outcome (records read, merged, expired, vectors applied).</returns>
    internal static async Task<RepoContextImportResult> ImportAsync(
        ILattice tree,
        Stream source,
        Serializer serializer,
        RepoContextSnapshotMerge? merge = null,
        RepoContextVectorImport? vectorImport = null,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(serializer);

        var mergeStrategy = merge ?? RepoContextRecordMerge.Default(serializer);
        var clock = timeProvider ?? TimeProvider.System;
        var reader = new RepoContextSnapshotReader(source, serializer);

        long read = 0;
        long merged = 0;
        long expired = 0;
        long vectorsApplied = 0;

        await foreach (var record in reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            read++;

            // One versioned read where the durable path used a plain one: the
            // existing value and its expiry arrive together, so reinstating the
            // time-to-live costs the import no extra round trip.
            var versioned = await tree
                .GetWithVersionAsync(record.Key, cancellationToken)
                .ConfigureAwait(false);
            var existing = versioned?.Value;

            // An absent key also reports zero ticks, and zero means durable to the
            // join - so joining against a key that is not there would read an
            // absence as a durable side and silently erase the record's expiry,
            // which is issue #2825 by another route. There is nothing to join with
            // unless the key is actually present.
            var effectiveExpiry = existing is null
                ? record.ExpiresAtTicks
                : JoinExpiry(versioned!.ExpiresAtTicks, record.ExpiresAtTicks);

            var nowTicks = clock.GetUtcNow().UtcTicks;
            if (effectiveExpiry != 0L && effectiveExpiry <= nowTicks)
            {
                expired++;
                continue;
            }

            var value = mergeStrategy(record.Key, existing, record.Value);
            if (effectiveExpiry == 0L)
            {
                await tree.SetAsync(record.Key, value, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await tree
                    .SetAsync(
                        record.Key,
                        value,
                        TimeSpan.FromTicks(effectiveExpiry - nowTicks),
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (existing is not null)
            {
                merged++;
            }

            if (record.Vector is not null && vectorImport is not null)
            {
                await vectorImport(
                        record.Key,
                        new RepoContextVectorPayload(record.Vector, record.EmbeddingSpace),
                        cancellationToken)
                    .ConfigureAwait(false);
                vectorsApplied++;
            }
        }

        return new RepoContextImportResult
        {
            FormatVersion = reader.FormatVersion,
            RecordsRead = read,
            RecordsMerged = merged,
            RecordsExpired = expired,
            VectorsApplied = vectorsApplied,
        };
    }

    /// <summary>
    /// Joins two absolute expiry instants (in UTC ticks, <c>0</c> meaning durable)
    /// the way the core value model joins them: durable wins over any finite
    /// expiry, and two finite expiries resolve to the later instant. The join is
    /// commutative, associative, and idempotent, so a re-import converges.
    /// <para>
    /// Both sides must be expiries that were actually observed on a live value.
    /// A key that is absent from the target also reports <c>0</c> ticks, and
    /// feeding that absence in here would be read as a durable side and would
    /// erase the incoming record's expiry - so the caller joins only when the
    /// existing value is present.
    /// </para>
    /// </summary>
    /// <param name="left">One expiry, in absolute UTC ticks, or <c>0</c> for durable.</param>
    /// <param name="right">The other expiry, in absolute UTC ticks, or <c>0</c> for durable.</param>
    private static long JoinExpiry(long left, long right) =>
        left == 0L || right == 0L ? 0L : Math.Max(left, right);

    /// <summary>
    /// The smallest key strictly greater than <paramref name="key"/> that still
    /// sorts immediately after it: <paramref name="key"/> with a NUL appended.
    /// Used as an exclusive-of-<paramref name="key"/> inclusive lower bound so a
    /// resumed scan skips the already-yielded key without missing its successors.
    /// </summary>
    /// <param name="key">The last key already yielded.</param>
    private static string Successor(string key) => key + '\0';

    /// <summary>
    /// The exclusive upper bound of the range covering all keys that start with
    /// <paramref name="prefix"/>: the prefix with its last character incremented,
    /// or <see langword="null"/> (open-ended) when no such bound exists (an empty
    /// prefix, or one that is all <see cref="char.MaxValue"/>).
    /// </summary>
    /// <param name="prefix">The key prefix.</param>
    internal static string? PrefixUpperBound(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] != char.MaxValue)
            {
                return string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i] + 1)).ToString());
            }
        }

        return null;
    }
}
