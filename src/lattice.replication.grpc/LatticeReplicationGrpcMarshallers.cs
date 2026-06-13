using System.Buffers;
using Grpc.Core;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Reference-typed wrapper around <see cref="ReplicationBatchEnvelope"/>.
/// gRPC's <see cref="Method{TRequest, TResponse}"/> imposes a
/// <c>class</c> constraint on its type parameters; the public
/// envelope is a <c>readonly record struct</c> for Orleans-serializer
/// reasons. The wrapper carries the value across the gRPC
/// boundary and is the only allocation the marshaller introduces per
/// call - the encoded payload itself is still written straight into
/// the gRPC stream's <see cref="System.Buffers.IBufferWriter{T}"/>.
/// <para>
/// Senders may populate either <see cref="Value"/> (the typed-envelope
/// path; the marshaller serializer writes the typed bytes via
/// <see cref="IReplicationBatchEncoder.Encode"/>) or <see cref="Framing"/>
/// (the framing-only path; the marshaller serializer writes the
/// framing bytes via
/// <see cref="IReplicationBatchEncoder.EncodeFraming"/>). Receivers
/// are agnostic: the deserializer always returns a box whose
/// <see cref="Value"/> is a fully-inflated
/// <see cref="ReplicationBatchEnvelope"/>, which is the contract the
/// receiver-side gRPC service consumes.
/// </para>
/// </summary>
internal sealed class ReplicationBatchEnvelopeBox
{
    public ReplicationBatchEnvelope Value { get; init; }

    /// <summary>
    /// Optional sender-side framing payload. When populated, the
    /// marshaller serializer writes the framing bytes verbatim via
    /// <see cref="IReplicationBatchEncoder.EncodeFraming"/> using the
    /// supplied routing strings, and the typed <see cref="Value"/> is
    /// not consulted. Always <see langword="null"/> on the receiver
    /// side: the deserializer surfaces the inflated
    /// <see cref="ReplicationBatchEnvelope"/> through <see cref="Value"/>
    /// regardless of the wire shape.
    /// </summary>
    public FramingPayload? Framing { get; init; }

    /// <summary>
    /// Sender-side framing payload bundled with the routing strings
    /// the framing wire format requires.
    /// </summary>
    internal readonly record struct FramingPayload(
        EncodedBatchHeader Header,
        string TreeName,
        string OriginClusterId,
        ReadOnlyMemory<ArraySegment<byte>> Entries);
}

/// <summary>
/// Reference-typed wrapper around <see cref="ReplicationAck"/>.
/// Mirrors <see cref="ReplicationBatchEnvelopeBox"/>; see that type's
/// remarks for rationale.
/// </summary>
internal sealed class ReplicationAckBox
{
    public ReplicationAck Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="DigestProbeRequest"/> for the
/// anti-entropy digest-probe RPC. gRPC's
/// <see cref="Method{TRequest, TResponse}"/> imposes a <c>class</c>
/// constraint; the public request is a <c>readonly record struct</c>.
/// Mirrors <see cref="ReplicationBatchEnvelopeBox"/>.
/// </summary>
internal sealed class DigestProbeRequestBox
{
    public DigestProbeRequest Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="DigestProbeResponse"/> for
/// the anti-entropy digest-probe RPC. Mirrors <see cref="ReplicationAckBox"/>.
/// </summary>
internal sealed class DigestProbeResponseBox
{
    public DigestProbeResponse Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="ContentManifestRequest"/> for
/// the content-hash payload-elision manifest-exchange RPC. gRPC's
/// <see cref="Method{TRequest, TResponse}"/> imposes a <c>class</c>
/// constraint; the public request is a <c>readonly record struct</c>.
/// Mirrors <see cref="DigestProbeRequestBox"/>.
/// </summary>
internal sealed class ContentManifestRequestBox
{
    public ContentManifestRequest Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="ContentManifestResponse"/> for
/// the content-hash payload-elision manifest-exchange RPC. Mirrors
/// <see cref="DigestProbeResponseBox"/>.
/// </summary>
internal sealed class ContentManifestResponseBox
{
    public ContentManifestResponse Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="CompressionDictionaryPullRequest"/>
/// for the self-distributing shared-dictionary pull RPC. gRPC's
/// <see cref="Method{TRequest, TResponse}"/> imposes a <c>class</c>
/// constraint; the public request is a <c>readonly record struct</c>.
/// Mirrors <see cref="ContentManifestRequestBox"/>.
/// </summary>
internal sealed class CompressionDictionaryPullRequestBox
{
    public CompressionDictionaryPullRequest Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="CompressionDictionaryPullResponse"/>
/// for the self-distributing shared-dictionary pull RPC. Mirrors
/// <see cref="ContentManifestResponseBox"/>.
/// </summary>
internal sealed class CompressionDictionaryPullResponseBox
{
    public CompressionDictionaryPullResponse Value { get; init; }
}

/// <summary>
/// Builds gRPC <see cref="Marshaller{T}"/> instances that delegate to
/// <see cref="IReplicationBatchEncoder"/> for the request envelope and
/// to the Orleans <see cref="Serializer{T}"/> for the response ack. The
/// factory is the single point that resolves the encoder/serializer
/// dependencies; the resulting marshallers are stateless and reused
/// across every gRPC call.
/// </summary>
/// <remarks>
/// The envelope serializer hands the gRPC
/// <see cref="SerializationContext.GetBufferWriter"/> straight through
/// to <see cref="IReplicationBatchEncoder.Encode"/>, so the envelope's
/// bytes never round-trip through a per-batch heap allocation - the
/// zero-allocation hot path the encoder seam was shaped for. The
/// deserializer prefers the single-segment fast path on
/// <see cref="ReadOnlySequence{T}.First"/>; multi-segment payloads are
/// materialised via a pooled buffer.
/// </remarks>
internal static class LatticeReplicationGrpcMarshallers
{
    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="ReplicationBatchEnvelopeBox"/> bound to the supplied
    /// <paramref name="encoder"/> and <paramref name="walRecordEncoder"/>.
    /// </summary>
    /// <param name="encoder">
    /// The replication batch encoder used for the typed-envelope
    /// path and for default framing encode/decode helpers.
    /// </param>
    /// <param name="walRecordEncoder">
    /// The WAL record encoder used to inflate framing-encoded entry
    /// segments back into <see cref="WalRecord"/> instances on the
    /// receiver side. Required so the framing-only fast path can
    /// surface a fully-typed <see cref="ReplicationBatchEnvelope"/>
    /// to the receiver service.
    /// </param>
    public static Marshaller<ReplicationBatchEnvelopeBox> CreateEnvelopeMarshaller(
        IReplicationBatchEncoder encoder,
        IWalRecordEncoder walRecordEncoder)
    {
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(walRecordEncoder);

        return Marshallers.Create<ReplicationBatchEnvelopeBox>(
            serializer: (box, context) =>
            {
                if (box.Framing is { } framing)
                {
                    // Framing-only fast path: write the framing bytes
                    // directly into the gRPC stream's buffer writer.
                    // The typed envelope is not consulted on this
                    // path; the receiver-side deserializer reconstructs
                    // it from the framing payload.
                    encoder.EncodeFraming(
                        framing.Header,
                        framing.TreeName,
                        framing.OriginClusterId,
                        framing.Entries,
                        context.GetBufferWriter());
                }
                else
                {
                    encoder.Encode(box.Value, context.GetBufferWriter());
                }
                context.Complete();
            },
            deserializer: context => new ReplicationBatchEnvelopeBox
            {
                Value = DecodeEnvelope(encoder, walRecordEncoder, context),
            });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="ReplicationAckBox"/> bound to the supplied Orleans
    /// <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the envelope marshaller.
    /// </summary>
    public static Marshaller<ReplicationAckBox> CreateAckMarshaller(Serializer<ReplicationAck> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<ReplicationAckBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new ReplicationAckBox { Value = DeserializeAck(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="DigestProbeRequestBox"/> bound to the supplied Orleans
    /// <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the ack marshaller.
    /// </summary>
    public static Marshaller<DigestProbeRequestBox> CreateProbeRequestMarshaller(Serializer<DigestProbeRequest> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<DigestProbeRequestBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new DigestProbeRequestBox { Value = DeserializeValue(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="DigestProbeResponseBox"/> bound to the supplied Orleans
    /// <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the ack marshaller.
    /// </summary>
    public static Marshaller<DigestProbeResponseBox> CreateProbeResponseMarshaller(Serializer<DigestProbeResponse> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<DigestProbeResponseBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new DigestProbeResponseBox { Value = DeserializeValue(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="ContentManifestRequestBox"/> bound to the supplied Orleans
    /// <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the probe marshallers.
    /// </summary>
    public static Marshaller<ContentManifestRequestBox> CreateContentManifestRequestMarshaller(Serializer<ContentManifestRequest> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<ContentManifestRequestBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new ContentManifestRequestBox { Value = DeserializeValue(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="ContentManifestResponseBox"/> bound to the supplied Orleans
    /// <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the probe marshallers.
    /// </summary>
    public static Marshaller<ContentManifestResponseBox> CreateContentManifestResponseMarshaller(Serializer<ContentManifestResponse> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<ContentManifestResponseBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new ContentManifestResponseBox { Value = DeserializeValue(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="CompressionDictionaryPullRequestBox"/> bound to the supplied
    /// Orleans <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the probe marshallers.
    /// </summary>
    public static Marshaller<CompressionDictionaryPullRequestBox> CreateCompressionDictionaryPullRequestMarshaller(Serializer<CompressionDictionaryPullRequest> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<CompressionDictionaryPullRequestBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new CompressionDictionaryPullRequestBox { Value = DeserializeValue(serializer, context) });
    }

    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for
    /// <see cref="CompressionDictionaryPullResponseBox"/> bound to the supplied
    /// Orleans <paramref name="serializer"/>. Uses the same buffer-writer
    /// hand-off pattern as the probe marshallers.
    /// </summary>
    public static Marshaller<CompressionDictionaryPullResponseBox> CreateCompressionDictionaryPullResponseMarshaller(Serializer<CompressionDictionaryPullResponse> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<CompressionDictionaryPullResponseBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new CompressionDictionaryPullResponseBox { Value = DeserializeValue(serializer, context) });
    }

    private static T DeserializeValue<T>(Serializer<T> serializer, GrpcDeserializationContext context)
    {
        var sequence = context.PayloadAsReadOnlySequence();
        if (sequence.IsSingleSegment)
        {
            return serializer.Deserialize(sequence.First.Span);
        }

        var length = checked((int)sequence.Length);
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            sequence.CopyTo(rented);
            return serializer.Deserialize(new ReadOnlySpan<byte>(rented, 0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static ReplicationBatchEnvelope DecodeEnvelope(
        IReplicationBatchEncoder encoder,
        IWalRecordEncoder walRecordEncoder,
        GrpcDeserializationContext context)
    {
        var sequence = context.PayloadAsReadOnlySequence();
        if (sequence.IsSingleSegment)
        {
            return DecodeEnvelopeFromMemory(encoder, walRecordEncoder, sequence.First);
        }

        var length = checked((int)sequence.Length);
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            sequence.CopyTo(rented);
            return DecodeEnvelopeFromMemory(encoder, walRecordEncoder, new ReadOnlyMemory<byte>(rented, 0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static ReplicationBatchEnvelope DecodeEnvelopeFromMemory(
        IReplicationBatchEncoder encoder,
        IWalRecordEncoder walRecordEncoder,
        ReadOnlyMemory<byte> payload)
    {
        // Framing-only fast path: detect the magic prefix before
        // touching the typed decoder. TryDecodeFraming returns false
        // (rather than throwing) on a magic mismatch, so a typed
        // payload still falls through to the typed decode below
        // without paying for an exception.
        if (encoder.TryDecodeFraming(
                payload,
                out var header,
                out var treeName,
                out var originClusterId,
                out var encodedEntries))
        {
            // Inflate the per-entry segments into WalRecord instances
            // so the receiver service consumes the existing typed
            // contract. This allocates one WalRecord[] plus the
            // per-entry WalRecord values; eliminating that allocation
            // is the next stage of the migration (a framing-aware
            // applier).
            var entries = new WalRecord[header.EntryCount];
            var segments = encodedEntries.Span;
            for (var i = 0; i < entries.Length; i++)
            {
                var seg = segments[i];
                // Re-stamp TreeId from the framing tail's TreeName
                // and Mode from the framing header's Mode field: the
                // producer stripped both slots at encode time because
                // they are batch-constant (TreeId since wire version
                // 4, Mode since wire version 5).
                entries[i] = walRecordEncoder.Decode(seg.AsSpan(), treeName, header.Mode);
            }

            return new ReplicationBatchEnvelope
            {
                WireVersion = ReplicationBatchEnvelope.CurrentVersion,
                TreeName = treeName,
                OriginClusterId = originClusterId,
                Entries = entries,
            };
        }

        return encoder.Decode(payload);
    }

    private static ReplicationAck DeserializeAck(Serializer<ReplicationAck> serializer, GrpcDeserializationContext context)
    {
        var sequence = context.PayloadAsReadOnlySequence();
        if (sequence.IsSingleSegment)
        {
            return serializer.Deserialize(sequence.First.Span);
        }

        var length = checked((int)sequence.Length);
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            sequence.CopyTo(rented);
            return serializer.Deserialize(new ReadOnlySpan<byte>(rented, 0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}

