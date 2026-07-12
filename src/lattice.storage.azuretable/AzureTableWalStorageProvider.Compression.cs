using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-row WAL payload compression for
/// <see cref="AzureTableWalStorageProvider"/>. Compression is opt-in
/// via <see cref="AzureTableWalStorageOptions.Compression"/> and is
/// applied per entity row so the read path's offset-addressable
/// <c>fromOffsetExclusive</c> projection still works without inflating
/// a whole batch to recover a single entry.
/// <para>
/// On-disk layout for a compressed row: the entity's
/// <see cref="AzureTableWalEntity.Compression"/> column carries the
/// algorithm tag and the <see cref="AzureTableWalEntity.Payload"/>
/// column holds <c>[4-byte little-endian uncompressed length][compressed
/// bytes]</c>. An uncompressed row tags <c>0</c>
/// (<see cref="LatticeCompression.None"/>) and stores the encoded bytes
/// verbatim, so a silo that never enabled compression - and any row
/// written before the column existed - reads back unmodified.
/// </para>
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    // Length, in bytes, of the little-endian uncompressed-length prefix
    // prepended to a compressed payload so the read path can size the
    // inflate buffer the ILatticeCompressor.Decompress contract requires.
    private const int CompressedLengthPrefixBytes = sizeof(int);

    // Upper bound on the uncompressed size the read path will materialise
    // from a single WAL row's declared length prefix. The prefix is read
    // from stored bytes that a compromised or buggy producer could forge;
    // without a ceiling a hostile row could declare a multi-gigabyte length
    // and drive the reader into an out-of-memory decompression bomb from a
    // few compressed bytes. 256 MiB is far above any legitimate single-row
    // WAL payload (batches are split well below this) while still bounding
    // the worst-case allocation.
    private const int MaxDecompressedRowBytes = 256 * 1024 * 1024;

    /// <summary>
    /// Builds the per-tag compressor dispatch dictionary from the DI
    /// sequence. Keyed by the raw <see cref="LatticeCompression"/> byte
    /// so host-defined algorithms (tags in <c>[0x80, 0xFF]</c>) round
    /// trip without a named enum member. Rejects a
    /// <see cref="LatticeCompression.None"/> registration (that tag is
    /// the reserved verbatim pass-through) and duplicate tags so a
    /// misconfigured host fails fast at construction.
    /// </summary>
    private static FrozenDictionary<byte, ILatticeCompressor> BuildCompressorDictionary(
        IEnumerable<ILatticeCompressor>? compressors)
    {
        if (compressors is null)
        {
            return FrozenDictionary<byte, ILatticeCompressor>.Empty;
        }

        var dict = new Dictionary<byte, ILatticeCompressor>();
        foreach (var compressor in compressors)
        {
            ArgumentNullException.ThrowIfNull(compressor);
            if (compressor.Algorithm == LatticeCompression.None)
            {
                throw new ArgumentException(
                    $"An {nameof(ILatticeCompressor)} cannot register {nameof(LatticeCompression)}.{nameof(LatticeCompression.None)}; "
                    + "that value is reserved for the uncompressed pass-through path.",
                    nameof(compressors));
            }
            var tag = (byte)compressor.Algorithm;
            if (!dict.TryAdd(tag, compressor))
            {
                throw new ArgumentException(
                    $"Multiple {nameof(ILatticeCompressor)} registrations for compression tag 0x{tag:X2} "
                    + $"({nameof(LatticeCompression)}.{compressor.Algorithm}); only one compressor may be registered per algorithm tag.",
                    nameof(compressors));
            }
        }
        return dict.ToFrozenDictionary();
    }

    /// <summary>
    /// Encodes the supplied entry payload for the row's
    /// <c>Payload</c> column, returning the bytes to store and the
    /// compression tag to record. When compression is disabled, the
    /// payload is shorter than
    /// <see cref="AzureTableWalStorageOptions.CompressionMinPayloadBytes"/>,
    /// or the payload is empty, the bytes are copied verbatim and the
    /// tag is <see cref="LatticeCompression.None"/>. Otherwise the
    /// active compressor compresses into a pooled buffer and the result
    /// is a freshly-owned <c>[4-byte LE uncompressed length][compressed
    /// bytes]</c> array tagged with the active algorithm.
    /// <para>
    /// If compressing does not actually shrink the payload - i.e. the
    /// compressed bytes plus the length prefix are not smaller than the
    /// input, as happens for incompressible data such as already-compressed
    /// blobs or random bytes - the payload is stored verbatim and tagged
    /// <see cref="LatticeCompression.None"/> instead. This guard keeps the
    /// stored row footprint at or below the uncompressed size and avoids
    /// paying a decompress cost on read for a saving that does not exist,
    /// which matters because compression is enabled by default.
    /// </para>
    /// </summary>
    private byte[] CompressPayload(ReadOnlySpan<byte> encoded, out byte compressionTag)
    {
        var compressor = _activeCompressor;
        if (compressor is null
            || encoded.Length == 0
            || encoded.Length < _compressionMinPayloadBytes)
        {
            compressionTag = (byte)LatticeCompression.None;
            return encoded.ToArray();
        }

        var maxCompressedLength = compressor.GetMaxCompressedLength(encoded.Length);
        var rented = ArrayPool<byte>.Shared.Rent(maxCompressedLength);
        try
        {
            var written = compressor.Compress(encoded, rented.AsSpan(0, maxCompressedLength));
            if (CompressedLengthPrefixBytes + written >= encoded.Length)
            {
                compressionTag = (byte)LatticeCompression.None;
                return encoded.ToArray();
            }

            var result = new byte[CompressedLengthPrefixBytes + written];
            BinaryPrimitives.WriteInt32LittleEndian(result, encoded.Length);
            rented.AsSpan(0, written).CopyTo(result.AsSpan(CompressedLengthPrefixBytes));
            compressionTag = _activeCompressionTag;
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Reverses <see cref="CompressPayload"/> for a row read back from
    /// the table. A <see cref="LatticeCompression.None"/> tag (the
    /// default, and the value a legacy row decodes to) returns the
    /// payload unchanged. A non-zero tag reads the little-endian
    /// uncompressed-length prefix, looks up the matching
    /// <see cref="ILatticeCompressor"/>, and inflates into a
    /// freshly-owned array of the recovered encoded bytes.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The row's compression tag has no registered compressor on this
    /// silo - the algorithm shipped without a coordinated upgrade.
    /// </exception>
    /// <exception cref="InvalidDataException">
    /// The compressed payload is shorter than its length prefix.
    /// </exception>
    private byte[]? DecompressPayload(byte[]? payload, byte compressionTag)
    {
        if (compressionTag == (byte)LatticeCompression.None || payload is null || payload.Length == 0)
        {
            return payload;
        }

        if (!_compressors.TryGetValue(compressionTag, out var compressor))
        {
            throw new NotSupportedException(
                $"WAL row carries compression tag 0x{compressionTag:X2} but no {nameof(ILatticeCompressor)} is registered for that algorithm on this silo. "
                + "Register the matching compressor (e.g. via AddLatticeCompressor) before reading rows written by a producer that enabled it.");
        }

        if (payload.Length < CompressedLengthPrefixBytes)
        {
            throw new InvalidDataException(
                $"Compressed WAL payload is {payload.Length} bytes, shorter than the {CompressedLengthPrefixBytes}-byte uncompressed-length prefix; the row is corrupt.");
        }

        var uncompressedLength = BinaryPrimitives.ReadInt32LittleEndian(payload);
        if (uncompressedLength < 0)
        {
            throw new InvalidDataException(
                $"Compressed WAL payload declares a negative uncompressed length ({uncompressedLength}); the row is corrupt.");
        }

        if (uncompressedLength > MaxDecompressedRowBytes)
        {
            throw new InvalidDataException(
                $"Compressed WAL payload declares an uncompressed length of {uncompressedLength} bytes, exceeding the "
                + $"{MaxDecompressedRowBytes}-byte decompression ceiling; the row is corrupt or hostile.");
        }

        var result = new byte[uncompressedLength];
        compressor.Decompress(
            payload.AsSpan(CompressedLengthPrefixBytes),
            result,
            uncompressedLength);
        return result;
    }

    /// <summary>
    /// Per-batch accumulator for the WAL compression-savings counters. Summed
    /// across a batch's entries by the encode helpers and emitted once on the
    /// <see cref="LatticeMetrics.Meter"/> after the batch's phase-1 rows land.
    /// </summary>
    internal struct WalCompressionStats
    {
        public long UncompressedBytes;
        public long StoredBytes;
        public int SkippedDisabled;
        public int SkippedBelowThreshold;
        public int SkippedInflationGuard;
    }

    /// <summary>
    /// Folds one encoded row into <paramref name="stats"/>: adds its pre- and
    /// post-compression byte lengths and, when the row was stored verbatim
    /// (tag <see cref="LatticeCompression.None"/>), attributes the skip to the
    /// reason it would have taken in <see cref="CompressPayload"/> - the same
    /// disabled / below-threshold / inflation-guard branches - derived from
    /// the tag, the uncompressed length, and the active-compressor
    /// configuration rather than threaded back out of the encode call.
    /// </summary>
    private void AccumulateCompressionStats(
        ref WalCompressionStats stats,
        byte compressionTag,
        long uncompressedLength,
        long storedLength)
    {
        stats.UncompressedBytes += uncompressedLength;
        stats.StoredBytes += storedLength;

        if (compressionTag != (byte)LatticeCompression.None)
        {
            return;
        }

        if (_activeCompressor is null)
        {
            stats.SkippedDisabled++;
        }
        else if (uncompressedLength < _compressionMinPayloadBytes)
        {
            stats.SkippedBelowThreshold++;
        }
        else
        {
            stats.SkippedInflationGuard++;
        }
    }

    /// <summary>
    /// Emits the WAL compression-savings counters for one append batch on the
    /// <see cref="LatticeMetrics.Meter"/>, tagged by tree. The two byte totals
    /// always emit (so a tree with traffic but no savings still reports);
    /// each skip-reason bucket emits only when non-zero.
    /// </summary>
    internal static void RecordWalCompressionMetrics(string treeId, in WalCompressionStats stats)
    {
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        LatticeMetrics.StorageWalUncompressedBytes.Add(stats.UncompressedBytes, treeTag);
        LatticeMetrics.StorageWalStoredBytes.Add(stats.StoredBytes, treeTag);

        if (stats.SkippedDisabled > 0)
        {
            LatticeMetrics.StorageWalCompressionSkipped.Add(
                stats.SkippedDisabled, treeTag, LatticeMetrics.ReasonCompressionDisabled);
        }
        if (stats.SkippedBelowThreshold > 0)
        {
            LatticeMetrics.StorageWalCompressionSkipped.Add(
                stats.SkippedBelowThreshold, treeTag, LatticeMetrics.ReasonBelowThreshold);
        }
        if (stats.SkippedInflationGuard > 0)
        {
            LatticeMetrics.StorageWalCompressionSkipped.Add(
                stats.SkippedInflationGuard, treeTag, LatticeMetrics.ReasonInflationGuard);
        }
    }
}
