using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Drains a snapshot cursor's raw entries page by page, streaming each page's
/// serialized bytes to the sink while accumulating the manifest metadata that
/// must be derived from the payload: the per-key descriptors, the running
/// content digest, the byte length, the chunk count, and the per-origin causal
/// high-water. The value payload is never buffered whole - only one page is held
/// at a time - while the descriptors (inherently part of the manifest) and the
/// small running aggregates accumulate.
/// </summary>
internal sealed class RawEntryCollector(Serializer serializer, BackupKeyMergeMode treeMergeMode)
{
    private readonly IncrementalHash _hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly List<BackupKeyDescriptor> _keyDescriptors = new();
    private readonly Dictionary<string, long> _perOriginHighWater = new(StringComparer.Ordinal);
    private long _byteLength;
    private int _chunkCount;
    private string? _contentHash;
    private int _unstampedOriginEntryCount;

    /// <summary>The per-key descriptors captured, in scan (ascending key) order.</summary>
    public IReadOnlyList<BackupKeyDescriptor> KeyDescriptors => _keyDescriptors;

    /// <summary>The per-origin causal high-water of the captured entries.</summary>
    public IReadOnlyDictionary<string, long> PerOriginHighWater => _perOriginHighWater;

    /// <summary>
    /// The number of captured entries that carried no origin stamp. Non-zero on a
    /// local-only (single-cluster) tree, whose rows are stamped with
    /// <see cref="string.Empty"/> by the core default origin resolver. Recorded so
    /// an empty <see cref="PerOriginHighWater"/> can be reported as the positive
    /// fact "every captured entry was locally authored" rather than as an
    /// indistinguishable absence.
    /// </summary>
    public int UnstampedOriginEntryCount => _unstampedOriginEntryCount;

    /// <summary>The total serialized byte length streamed to the sink.</summary>
    public long ByteLength => _byteLength;

    /// <summary>The number of chunks (drained pages) streamed to the sink.</summary>
    public int ChunkCount => _chunkCount;

    /// <summary>
    /// The lowercase hexadecimal SHA-256 content address of the streamed payload.
    /// Available only after <see cref="StreamAsync"/> has been fully enumerated.
    /// </summary>
    public string ContentHash =>
        _contentHash ?? throw new InvalidOperationException(
            "The content hash is not available until the raw-entry stream has been fully drained.");

    /// <summary>
    /// Streams the cursor's raw entries as an ordered sequence of serialized page
    /// chunks, updating the accumulated manifest metadata as each page passes
    /// through. Finalizes <see cref="ContentHash"/> when the cursor is drained.
    /// </summary>
    /// <param name="cursor">The snapshot cursor grain to drain.</param>
    /// <param name="pageSize">The number of raw entries to request per round-trip.</param>
    /// <param name="cancellationToken">Cancels the drain.</param>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> StreamAsync(
        ILatticeCursorGrain cursor,
        int pageSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await cursor.NextRawEntriesAsync(pageSize).ConfigureAwait(false);

            if (page.Entries.Count > 0)
            {
                foreach (var entry in page.Entries)
                {
                    RecordEntry(entry);
                }

                var entries = page.Entries as LwwEntry[] ?? page.Entries.ToArray();
                var bytes = serializer.SerializeToArray(entries);
                _hasher.AppendData(bytes);
                _byteLength += bytes.Length;
                _chunkCount++;
                yield return bytes;
            }

            if (!page.HasMore)
            {
                break;
            }
        }

        _contentHash = Convert.ToHexStringLower(_hasher.GetHashAndReset());
        _hasher.Dispose();
    }

    private void RecordEntry(LwwEntry entry)
    {
        // Prefer the durable per-key merge-mode discriminator carried on the
        // snapshot row: a local-only tree that mixes LWW and CRDT keys labels
        // each key with its true mode this way. When the discriminator is
        // absent (a plain last-writer-wins key, or a row persisted before the
        // discriminator existed), fall back to the declared tree mode - the
        // coarse per-tree labelling shipped by the original full-capture
        // engine, preserved verbatim so legacy snapshots are unaffected. The
        // captured VALUE bytes are the faithful post-merge (converged) state in
        // every case.
        var mergeMode = entry.MergeMode is { } perKeyMode
            ? (perKeyMode == LatticeMergeMode.LwwRegister
                ? BackupKeyMergeMode.LastWriterWins
                : BackupKeyMergeMode.Crdt)
            : treeMergeMode;

        // An unstamped origin arrives as string.Empty, NOT null. The core default
        // resolver (DefaultLatticeOriginClusterIdResolver) returns string.Empty for
        // every tree on a single-cluster host, and its contract says downstream
        // consumers ignore it - so normalize here, exactly as the incremental path
        // (IncrementalDeltaCollector.OnEntry) already does.
        //
        // Guarding the raw value with `is { }` filters null but admits "", which
        // then becomes a per-origin high-water key, and those keys become
        // BackupOriginProvenance.OriginId - which rejects empty. That threw on every
        // full capture of any tree without replication configured (#2621), i.e. the
        // default local deployment. It also silently seeded a ""-keyed frontier into
        // BackupConsistencyCut, and wrote "" into BackupKeyDescriptor.OriginId,
        // which is documented as null for a single-origin tree.
        var origin = string.IsNullOrEmpty(entry.OriginClusterId) ? null : entry.OriginClusterId;

        _keyDescriptors.Add(new BackupKeyDescriptor(
            entry.Key,
            mergeMode,
            origin));

        if (origin is { } originId)
        {
            var ticks = entry.Timestamp.WallClockTicks;
            if (ticks < 0)
            {
                ticks = 0;
            }
            if (!_perOriginHighWater.TryGetValue(originId, out var current) || ticks > current)
            {
                _perOriginHighWater[originId] = ticks;
            }
        }
        else
        {
            _unstampedOriginEntryCount++;
        }
    }
}
