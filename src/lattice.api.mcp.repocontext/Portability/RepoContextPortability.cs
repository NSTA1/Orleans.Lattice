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
    /// </summary>
    /// <param name="tree">The Lattice tree to enumerate. Must not be <see langword="null"/>.</param>
    /// <param name="prefix">The key prefix that bounds the enumeration. Must not be <see langword="null"/>.</param>
    /// <param name="continuationToken">A token from a prior page to resume after, or <see langword="null"/> to start at the prefix.</param>
    /// <param name="pageSize">The maximum number of records to return. Must be positive.</param>
    /// <param name="vectorExport">An optional resolver for each record's opaque vector payload, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>A page of records with a continuation token and a has-more flag.</returns>
    internal static async Task<RepoContextSnapshotPage> EnumerateAsync(
        ILattice tree,
        string prefix,
        string? continuationToken,
        int pageSize,
        RepoContextVectorExport? vectorExport,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var startInclusive = continuationToken is null ? prefix : Successor(continuationToken);
        var endExclusive = PrefixUpperBound(prefix);

        var cursorId = await tree
            .OpenEntryCursorAsync(startInclusive, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        LatticeCursorEntriesPage page;
        try
        {
            page = await tree
                .NextEntriesAsync(cursorId, pageSize, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId, cancellationToken).ConfigureAwait(false);
        }

        var records = new List<RepoContextSnapshotRecord>(page.Entries.Count);
        string? lastKey = null;
        foreach (var entry in page.Entries)
        {
            lastKey = entry.Key;
            RepoContextVectorPayload? vector = vectorExport is null
                ? null
                : await vectorExport(entry.Key, cancellationToken).ConfigureAwait(false);

            records.Add(new RepoContextSnapshotRecord
            {
                Key = entry.Key,
                Value = entry.Value,
                Vector = vector?.Vector,
                EmbeddingSpace = vector?.EmbeddingSpace,
            });
        }

        var hasMore = page.HasMore && lastKey is not null;
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
            var page = await EnumerateAsync(tree, prefix, token, pageSize, vectorExport, cancellationToken)
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
    /// </summary>
    /// <param name="tree">The Lattice tree to import into. Must not be <see langword="null"/>.</param>
    /// <param name="source">The snapshot stream to read. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer for snapshot records. Must not be <see langword="null"/>.</param>
    /// <param name="merge">The CRDT merge strategy, or <see langword="null"/> to use the record model's default.</param>
    /// <param name="vectorImport">An optional sink for each record's opaque vector payload, or <see langword="null"/> to ignore vectors.</param>
    /// <param name="cancellationToken">Cancels the import.</param>
    /// <returns>The import outcome (records read, records merged, vectors applied).</returns>
    internal static async Task<RepoContextImportResult> ImportAsync(
        ILattice tree,
        Stream source,
        Serializer serializer,
        RepoContextSnapshotMerge? merge = null,
        RepoContextVectorImport? vectorImport = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(serializer);

        var mergeStrategy = merge ?? RepoContextRecordMerge.Default(serializer);
        var reader = new RepoContextSnapshotReader(source, serializer);

        long read = 0;
        long merged = 0;
        long vectorsApplied = 0;

        await foreach (var record in reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            read++;

            var existing = await tree.GetAsync(record.Key, cancellationToken).ConfigureAwait(false);
            var value = mergeStrategy(record.Key, existing, record.Value);
            await tree.SetAsync(record.Key, value, cancellationToken).ConfigureAwait(false);
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
            VectorsApplied = vectorsApplied,
        };
    }

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
