using System;
using System.Buffers.Binary;
using System.Text;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Fixed-shape framing header that prefixes a batch of pre-encoded
/// <see cref="BPlusTree.Grains.WalRecord"/> bytes when the framing-only
/// encoder seam
/// (<see cref="IReplicationBatchEncoder.EncodeFraming(in EncodedBatchHeader, System.ReadOnlyMemory{System.ArraySegment{byte}}, System.Buffers.IBufferWriter{byte})"/>
/// and
/// <see cref="IReplicationBatchEncoder.TryDecodeFraming(System.ReadOnlyMemory{byte}, out EncodedBatchHeader, out System.ReadOnlyMemory{System.ArraySegment{byte}})"/>)
/// is in play. The header carries only batch-level routing metadata;
/// per-entry fields stay inside each entry's encoded bytes so the
/// framing layer can advance opaque byte segments without ever
/// materialising a <see cref="BPlusTree.Grains.WalRecord"/>.
/// <para>
/// The header has a deliberate fixed 32-byte wire layout (see
/// <see cref="WireSize"/>) so a receiver can read it with a single
/// fixed-size buffer fill before deciding how to allocate for the
/// variable-length tail. The fields are written little-endian, matching
/// the dominant convention of every other wire-format value type in
/// the repository.
/// </para>
/// <para>
/// This is an in-process value type, not an Orleans-serialisable
/// envelope: the encoder writes the bytes directly via
/// <see cref="System.Buffers.IBufferWriter{T}"/> rather than running it
/// through the Orleans serializer. The Orleans-serialised envelope
/// shape (<see cref="ReplicationBatchEnvelope"/>) is preserved verbatim
/// for transports that still consume strongly-typed entries.
/// </para>
/// </summary>
public readonly record struct EncodedBatchHeader
{
    /// <summary>
    /// Total size of the fixed-shape header in bytes, written verbatim
    /// at the front of every framing-encoded batch. Receivers fill a
    /// stack-allocated span of this length on the first read before
    /// touching the variable-length tail.
    /// </summary>
    public const int WireSize = 32;

    /// <summary>
    /// Magic prefix written at offset 0 of every framing-encoded
    /// batch. Spells "OLRF" (Orleans Lattice Replication Framing) when
    /// the four little-endian bytes are read as ASCII, so an operator
    /// dumping the first 4 bytes of a captured batch sees a
    /// recognisable signature rather than an opaque hash.
    /// </summary>
    public const uint MagicValue = 0x46524C4Fu;

    /// <summary>
    /// Wire-format version stamped by the canonical framing encoder.
    /// Bumped on every breaking change to the framing layout; consumers
    /// compare strictly greater-than for rejection.
    /// </summary>
    public const int CurrentWireVersion = 2;

    /// <summary>
    /// Magic prefix. Must equal <see cref="MagicValue"/> on any batch
    /// decoded by the canonical receiver; receivers fail fast with a
    /// descriptive error rather than guessing at the layout when the
    /// prefix does not match.
    /// </summary>
    public uint Magic { get; init; }

    /// <summary>
    /// Framing wire-format version. Receivers reject payloads strictly
    /// greater than the version they were built against.
    /// </summary>
    public int WireVersion { get; init; }

    /// <summary>
    /// Stable non-cryptographic hash of the origin cluster id (UTF-8
    /// bytes of <see cref="ReplicationBatch.OriginClusterId"/>, FNV-1a
    /// 64-bit). Lets the receiver cross-check the surrounding
    /// call-shape <see cref="ReplicationBatch.OriginClusterId"/>
    /// without re-shipping the variable-length string inside the
    /// fixed header. A mismatch indicates either a misrouted batch or
    /// a producer-side bug; receivers fail fast.
    /// </summary>
    public ulong OriginClusterIdHash { get; init; }

    /// <summary>
    /// Number of pre-encoded entry segments that follow the header.
    /// Each entry is written as a 4-byte little-endian length prefix
    /// followed by exactly that many encoded bytes.
    /// </summary>
    public int EntryCount { get; init; }

    /// <summary>
    /// Monotonic batch sequence number stamped by the shipper. Used by
    /// the receiver to dedup re-shipped batches and to surface
    /// out-of-order delivery on the inbound metric path.
    /// </summary>
    public long BatchSequence { get; init; }

    /// <summary>
    /// Number of atomic-batch spans carried in the variable-length
    /// span table that follows the entry segments. Reserved for the
    /// receiver-side dispatcher that has to surface saga / prepared
    /// markers separately from individual entries; the producer-side
    /// shipper sets this to <c>0</c> until the dispatcher is wired in
    /// (the per-entry <c>AtomicBatchSize</c> / <c>AtomicBatchIndex</c>
    /// / <c>TransactionId</c> slots on
    /// <see cref="BPlusTree.Grains.WalRecord"/> remain the source of
    /// truth in the interim).
    /// </summary>
    public int AtomicBatchSpanCount { get; init; }

    /// <summary>
    /// Compression algorithm applied to the entry-segment bytes after
    /// the header. Reserved for a follow-on roadmap entry; the current
    /// canonical encoder sets this to <see cref="FramingCompression.None"/>
    /// on every batch.
    /// </summary>
    public FramingCompression Compression { get; init; }

    /// <summary>
    /// Writes this header into the supplied <paramref name="destination"/>
    /// span using the canonical little-endian wire layout. The span
    /// must be at least <see cref="WireSize"/> bytes long.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="destination"/> is shorter than
    /// <see cref="WireSize"/>.
    /// </exception>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < WireSize)
        {
            throw new ArgumentException(
                $"Destination span must be at least {WireSize} bytes; got {destination.Length}.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[0..4], Magic);
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..8], WireVersion);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..16], OriginClusterIdHash);
        BinaryPrimitives.WriteInt32LittleEndian(destination[16..20], EntryCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[20..28], BatchSequence);
        // The trailing 4-byte slot packs AtomicBatchSpanCount (low 24
        // bits) and the Compression enum (top 8 bits). Span counts are
        // bounded by per-batch entry count and easily fit in 24 bits;
        // compression enum values fit in a byte.
        var spanCount = AtomicBatchSpanCount;
        if (spanCount < 0 || spanCount > 0x00FFFFFF)
        {
            throw new InvalidOperationException(
                $"{nameof(AtomicBatchSpanCount)} must fit in 24 bits; got {spanCount}.");
        }
        var compression = (uint)(byte)Compression;
        var packed = ((uint)spanCount & 0x00FFFFFFu) | (compression << 24);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[28..32], packed);
    }

    /// <summary>
    /// Reads a header from the supplied <paramref name="source"/>
    /// span. The span must be at least <see cref="WireSize"/> bytes
    /// long; only the first <see cref="WireSize"/> bytes are consumed.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="source"/> is shorter than
    /// <see cref="WireSize"/>.
    /// </exception>
    public static EncodedBatchHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < WireSize)
        {
            throw new ArgumentException(
                $"Source span must be at least {WireSize} bytes; got {source.Length}.",
                nameof(source));
        }

        var packed = BinaryPrimitives.ReadUInt32LittleEndian(source[28..32]);
        return new EncodedBatchHeader
        {
            Magic = BinaryPrimitives.ReadUInt32LittleEndian(source[0..4]),
            WireVersion = BinaryPrimitives.ReadInt32LittleEndian(source[4..8]),
            OriginClusterIdHash = BinaryPrimitives.ReadUInt64LittleEndian(source[8..16]),
            EntryCount = BinaryPrimitives.ReadInt32LittleEndian(source[16..20]),
            BatchSequence = BinaryPrimitives.ReadInt64LittleEndian(source[20..28]),
            AtomicBatchSpanCount = (int)(packed & 0x00FFFFFFu),
            Compression = (FramingCompression)(byte)(packed >> 24),
        };
    }

    /// <summary>
    /// Computes the stable FNV-1a 64-bit hash of the supplied cluster
    /// id's UTF-8 bytes. Used to derive
    /// <see cref="OriginClusterIdHash"/> from a cluster id string
    /// without bringing in a cryptographic hash dependency or
    /// allocating per call.
    /// </summary>
    public static ulong HashClusterId(string clusterId)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        const ulong FnvOffsetBasis = 14695981039346656037UL;
        const ulong FnvPrime = 1099511628211UL;
        var hash = FnvOffsetBasis;
        var maxBytes = Encoding.UTF8.GetMaxByteCount(clusterId.Length);
        Span<byte> buffer = maxBytes <= 256
            ? stackalloc byte[256]
            : new byte[maxBytes];
        var written = Encoding.UTF8.GetBytes(clusterId, buffer);
        for (var i = 0; i < written; i++)
        {
            hash ^= buffer[i];
            hash *= FnvPrime;
        }
        return hash;
    }
}

/// <summary>
/// Compression algorithm applied to a framing-encoded batch's entry
/// segments. Reserved for a follow-on roadmap entry; the canonical
/// encoder currently writes <see cref="None"/> on every batch.
/// </summary>
public enum FramingCompression : byte
{
    /// <summary>
    /// Entry segments are written verbatim; no compression layer is
    /// applied. The only value the current canonical encoder writes.
    /// </summary>
    None = 0,
}