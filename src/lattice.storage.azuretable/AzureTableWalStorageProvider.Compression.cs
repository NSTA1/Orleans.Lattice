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

        var result = new byte[uncompressedLength];
        compressor.Decompress(
            payload.AsSpan(CompressedLengthPrefixBytes),
            result,
            uncompressedLength);
        return result;
    }
}
