using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationBatchEncoder"/> implementation.
/// Frames a <see cref="ReplicationBatchEnvelope"/> using the Orleans
/// serializer, producing the canonical binary wire format
/// <c>application/x-orleans-lattice-replog+binary</c>. Stamps
/// <see cref="ReplicationBatchEnvelope.CurrentVersion"/> on outbound
/// envelopes whose <see cref="ReplicationBatchEnvelope.WireVersion"/>
/// is the default <c>0</c>; rejects inbound payloads whose
/// <see cref="ReplicationBatchEnvelope.WireVersion"/> is strictly
/// greater than the supported version.
/// <para>
/// The Orleans serializer's <c>byte[]</c> handling is roughly 33% more
/// compact than JSON's base64 encoding on the same payload, which is
/// the bandwidth case the binary-framing seam exists to address. A
/// JSON encoder remains a future option for HTTP-transport
/// debuggability and is wired in by registering an alternative
/// <see cref="IReplicationBatchEncoder"/> via DI.
/// </para>
/// </summary>
internal sealed class OrleansBinaryReplicationBatchEncoder : IReplicationBatchEncoder
{
    /// <summary>
    /// Canonical binary content type stamped on outbound HTTP / gRPC
    /// metadata. The <c>+binary</c> suffix mirrors the convention from
    /// <c>application/foo+json</c> media types so dispatch tables that
    /// match on the <c>+xxx</c> suffix can route to the Orleans
    /// serializer codec without parsing the prefix.
    /// </summary>
    public const string BinaryContentType = "application/x-orleans-lattice-replog+binary";

    private readonly Serializer<ReplicationBatchEnvelope> _serializer;

    /// <summary>
    /// Initialises the encoder with the supplied
    /// <see cref="Serializer{T}"/>. Resolved from DI in the standard
    /// silo registration path; tests construct it directly with a
    /// serializer pulled from
    /// <c>new ServiceCollection().AddSerializer().BuildServiceProvider()</c>.
    /// </summary>
    public OrleansBinaryReplicationBatchEncoder(Serializer<ReplicationBatchEnvelope> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
    }

    /// <inheritdoc />
    public string ContentType => BinaryContentType;

    /// <inheritdoc />
    public int CurrentWireVersion => ReplicationBatchEnvelope.CurrentVersion;

    /// <inheritdoc />
    public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        if (string.IsNullOrEmpty(envelope.TreeName))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.TreeName)} must be non-empty.",
                nameof(envelope));
        }

        if (string.IsNullOrEmpty(envelope.OriginClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.OriginClusterId)} must be non-empty.",
                nameof(envelope));
        }

        if (envelope.WireVersion < 0)
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.WireVersion)} must be non-negative.",
                nameof(envelope));
        }

        // Stamp the current wire version when the caller left it at the
        // default 0; preserve any explicitly-supplied value verbatim so
        // tests and forward-compat producers can author payloads
        // targeting a specific version without the encoder silently
        // overwriting them. Normalise a null Entries collection to an
        // empty array so receivers never have to special-case null.
        var stamped = envelope with
        {
            WireVersion = envelope.WireVersion == 0 ? CurrentWireVersion : envelope.WireVersion,
            Entries = envelope.Entries ?? Array.Empty<ReplogEntry>(),
        };

        // Hand the buffer writer straight to the Orleans serializer so
        // the envelope's bytes are appended into caller-owned memory
        // (typically a pooled ArrayBufferWriter, or the gRPC stream's
        // writer at the transport layer). No per-batch byte[]
        // allocation in the canonical hot path.
        _serializer.Serialize(stamped, writer);
    }

    /// <inheritdoc />
    public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty)
        {
            throw new ArgumentException(
                "Replication batch payload must be non-empty.",
                nameof(payload));
        }

        ReplicationBatchEnvelope envelope;
        try
        {
            // Serializer<T>.Deserialize accepts ReadOnlySpan<byte>; the
            // caller's ReadOnlyMemory is materialised via .Span so we
            // do not allocate a copy.
            envelope = _serializer.Deserialize(payload.Span);
        }
        catch (Exception inner)
        {
            throw new ArgumentException(
                "Replication batch payload could not be decoded; the bytes are not a valid "
                + $"{nameof(ReplicationBatchEnvelope)} produced by this encoder.",
                nameof(payload),
                inner);
        }

        if (envelope.WireVersion > CurrentWireVersion)
        {
            throw new NotSupportedException(
                $"Replication batch envelope wire version {envelope.WireVersion} is newer than "
                + $"the supported version {CurrentWireVersion}; upgrade the receiver before "
                + "applying payloads from this producer.");
        }

        // Defensive normalisation: a hand-constructed payload may have
        // been encoded with Entries left at the default null. Receivers
        // expect an iterable; substitute an empty list so call sites do
        // not have to add a null guard.
        if (envelope.Entries is null)
        {
            envelope = envelope with { Entries = Array.Empty<ReplogEntry>() };
        }

        return envelope;
    }
}
