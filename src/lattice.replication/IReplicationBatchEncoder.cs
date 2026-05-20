using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using System.Buffers.Binary;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Pluggable seam for encoding and decoding the on-the-wire payload
/// supplied to <see cref="IReplicationTransport.SendAsync(ReplicationBatch, CancellationToken)"/>.
/// Implementations frame a batch of <see cref="WalRecord"/> records
/// inside a versioned <see cref="ReplicationBatchEnvelope"/>; the
/// transport treats the resulting bytes as opaque and never inspects
/// them.
/// <para>
/// The default DI registration is
/// <see cref="OrleansBinaryReplicationBatchEncoder"/>, which uses the
/// Orleans serializer as the canonical wire format. Hosts that need a
/// different framing - JSON for debuggability over an HTTP transport,
/// a custom envelope for compatibility with an external pipeline,
/// content-hash-prefixed framing for deduplication - replace the
/// registration via standard DI:
/// </para>
/// <code>
/// services.AddSingleton&lt;IReplicationBatchEncoder, MyEncoder&gt;();
/// </code>
/// <para>
/// Implementations are expected to be safe for concurrent invocation
/// from multiple threads; the canonical Orleans-serializer-backed
/// implementation is, because the underlying <c>Serializer&lt;T&gt;</c>
/// is thread-safe by contract.
/// </para>
/// <para>
/// <b>Allocation contract.</b> The encode path is deliberately
/// expressed in terms of <see cref="IBufferWriter{T}"/> rather than a
/// freshly-allocated <c>byte[]</c>: the canonical streaming push
/// transport hands the gRPC stream's writer in directly so the
/// envelope's bytes never round-trip through a per-batch heap
/// allocation. Callers that need a materialised buffer (tests,
/// debug-tooling, in-process loopback transports) supply an
/// <see cref="ArrayBufferWriter{T}"/> and read
/// <see cref="ArrayBufferWriter{T}.WrittenMemory"/>; the writer's
/// lifetime is the caller's responsibility, which matches the
/// ownership model
/// <see cref="ReplicationBatch.Payload"/> already imposes on the bytes
/// it carries.
/// </para>
/// </summary>
public interface IReplicationBatchEncoder
{
    /// <summary>
    /// Stable identifier for the wire format this encoder produces,
    /// suitable for use as an HTTP <c>Content-Type</c> header or a gRPC
    /// metadata tag. Receivers may use this value to dispatch among
    /// multiple registered encoders (e.g. binary by default, JSON when
    /// a debugging flag is set).
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// The wire-format version this encoder authors when calling
    /// <see cref="Encode"/>. Stamped on every produced
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> when the caller
    /// left it at the default <c>0</c>; compared strictly against
    /// incoming values during <see cref="Decode"/> (greater than is
    /// rejected, less-than-or-equal is accepted).
    /// </summary>
    int CurrentWireVersion { get; }

    /// <summary>
    /// Encodes the supplied <paramref name="envelope"/> into
    /// <paramref name="writer"/>. Implementations stamp
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> with their
    /// own <see cref="CurrentWireVersion"/> if the caller left it at
    /// the default <c>0</c>, but must not silently downgrade or
    /// upgrade an explicitly-supplied non-zero version.
    /// <para>
    /// The encoder appends bytes to <paramref name="writer"/> via the
    /// standard <see cref="IBufferWriter{T}"/> contract
    /// (<c>GetSpan</c> / <c>Advance</c>); it does not reset, rewind,
    /// or otherwise mutate any bytes already written by an earlier
    /// call. Callers that expect a single-batch buffer must supply a
    /// fresh writer per call.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="writer"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the envelope is missing required routing metadata
    /// (<see cref="ReplicationBatchEnvelope.TreeName"/> or
    /// <see cref="ReplicationBatchEnvelope.OriginClusterId"/> null or
    /// empty) or carries a negative
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/>.
    /// </exception>
    void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer);

    /// <summary>
    /// Decodes the supplied <paramref name="payload"/> back into a
    /// <see cref="ReplicationBatchEnvelope"/>.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="payload"/> is empty or malformed
    /// (the underlying serializer's exception is wrapped or surfaced as
    /// the implementation sees fit).
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Thrown when the decoded payload's
    /// <see cref="ReplicationBatchEnvelope.WireVersion"/> is strictly
    /// greater than <see cref="CurrentWireVersion"/>; the receiver
    /// fails fast rather than guess at the layout.
    /// </exception>
    ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload);

    /// <summary>
    /// Writes the supplied fixed-shape <paramref name="header"/>
    /// followed by length-prefixed <paramref name="entries"/> segments
    /// into <paramref name="writer"/>. This is the framing-only fast
    /// path used by transports that consume
    /// <see cref="ReplicationBatch.EncodedEnvelope"/>: each entry is
    /// already a pre-encoded <see cref="WalRecord"/> byte segment, so
    /// the encoder writes the bytes verbatim and never materialises a
    /// strongly-typed entry. The wire layout is:
    /// <list type="number">
    /// <item><description>The header bytes (<see cref="EncodedBatchHeader.WireSize"/> bytes, little-endian, see <see cref="EncodedBatchHeader.WriteTo(Span{byte})"/>).</description></item>
    /// <item><description>For each entry segment, a 4-byte little-endian length prefix followed by the segment bytes verbatim.</description></item>
    /// </list>
    /// <para>
    /// Default implementation on the interface writes the canonical
    /// layout above using <see cref="EncodedBatchHeader.WriteTo(Span{byte})"/>;
    /// custom encoders override only when they need a different
    /// framing.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="writer"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="header"/>'s
    /// <see cref="EncodedBatchHeader.EntryCount"/> does not match the
    /// length of <paramref name="entries"/>.
    /// </exception>
    void EncodeFraming(
        in EncodedBatchHeader header,
        ReadOnlyMemory<ArraySegment<byte>> entries,
        IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        if (header.EntryCount != entries.Length)
        {
            throw new ArgumentException(
                $"{nameof(EncodedBatchHeader)}.{nameof(EncodedBatchHeader.EntryCount)} "
                + $"({header.EntryCount}) does not match entries.Length ({entries.Length}).",
                nameof(header));
        }

        var headerSpan = writer.GetSpan(EncodedBatchHeader.WireSize);
        header.WriteTo(headerSpan);
        writer.Advance(EncodedBatchHeader.WireSize);

        var segments = entries.Span;
        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            var lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, segment.Count);
            writer.Advance(4);

            if (segment.Count > 0)
            {
                var dest = writer.GetSpan(segment.Count);
                segment.AsSpan().CopyTo(dest);
                writer.Advance(segment.Count);
            }
        }
    }

    /// <summary>
    /// Attempts to decode a framing-encoded payload back into its
    /// <see cref="EncodedBatchHeader"/> and pre-encoded entry segments.
    /// Returns <see langword="true"/> on success; <see langword="false"/>
    /// when the magic prefix does not match or the payload is too
    /// short for the fixed header. A wire-version mismatch is surfaced
    /// as a thrown <see cref="NotSupportedException"/> so the receiver
    /// can distinguish "not a framing-encoded payload" (caller should
    /// fall back to the typed decode) from "definitely a framing
    /// payload but built against a newer version".
    /// <para>
    /// The returned <paramref name="entries"/> memory references owned
    /// <see cref="ArraySegment{T}"/> wrappers that point back into
    /// <paramref name="payload"/>; do not retain them past the
    /// lifetime of the surrounding payload buffer.
    /// </para>
    /// <para>
    /// Default implementation parses the canonical layout produced by
    /// <see cref="EncodeFraming"/>; custom encoders override only when
    /// they need a different framing.
    /// </para>
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// Thrown when the payload's framing wire version is strictly
    /// greater than <see cref="EncodedBatchHeader.CurrentWireVersion"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the payload is truncated mid-entry (the header
    /// promises more bytes than the payload contains).
    /// </exception>
    bool TryDecodeFraming(
        ReadOnlyMemory<byte> payload,
        out EncodedBatchHeader header,
        out ReadOnlyMemory<ArraySegment<byte>> entries)
    {
        header = default;
        entries = ReadOnlyMemory<ArraySegment<byte>>.Empty;

        if (payload.Length < EncodedBatchHeader.WireSize)
        {
            return false;
        }

        var span = payload.Span;
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(span[0..4]);
        if (magic != EncodedBatchHeader.MagicValue)
        {
            return false;
        }

        var parsed = EncodedBatchHeader.ReadFrom(span);
        if (parsed.WireVersion > EncodedBatchHeader.CurrentWireVersion)
        {
            throw new NotSupportedException(
                $"Framing wire version {parsed.WireVersion} is newer than the supported "
                + $"version {EncodedBatchHeader.CurrentWireVersion}; upgrade the receiver "
                + "before applying payloads from this producer.");
        }

        if (parsed.EntryCount < 0)
        {
            throw new ArgumentException(
                $"Framing header reports a negative entry count ({parsed.EntryCount}); "
                + "payload is corrupt.",
                nameof(payload));
        }

        var segments = parsed.EntryCount == 0
            ? Array.Empty<ArraySegment<byte>>()
            : new ArraySegment<byte>[parsed.EntryCount];

        // Resolve the payload to a contiguous byte[] so we can wrap
        // each entry as an ArraySegment that points back into it
        // without copying. ReadOnlyMemory<byte> backed by an array
        // exposes its segment via MemoryMarshal.TryGetArray; the
        // canonical caller (gRPC marshaller, in-memory buffer) always
        // wraps a byte[], so this fast path covers every production
        // case. For the rare non-array-backed memory, fall back to
        // copying into a single buffer.
        byte[] backing;
        int backingOffset;
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(payload, out var seg)
            && seg.Array is { } backingArray)
        {
            backing = backingArray;
            backingOffset = seg.Offset;
        }
        else
        {
            backing = payload.ToArray();
            backingOffset = 0;
        }

        var cursor = EncodedBatchHeader.WireSize;
        for (var i = 0; i < parsed.EntryCount; i++)
        {
            if (cursor + 4 > payload.Length)
            {
                throw new ArgumentException(
                    $"Framing payload is truncated at the length prefix for entry {i} of "
                    + $"{parsed.EntryCount}; expected at least 4 more bytes at offset {cursor}.",
                    nameof(payload));
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
            cursor += 4;
            if (length < 0 || cursor + length > payload.Length)
            {
                throw new ArgumentException(
                    $"Framing payload is truncated at the body for entry {i} of "
                    + $"{parsed.EntryCount}; declared length {length} would overrun the "
                    + $"payload (remaining {payload.Length - cursor} bytes).",
                    nameof(payload));
            }
            segments[i] = new ArraySegment<byte>(backing, backingOffset + cursor, length);
            cursor += length;
        }

        header = parsed;
        entries = segments;
        return true;
    }
}
