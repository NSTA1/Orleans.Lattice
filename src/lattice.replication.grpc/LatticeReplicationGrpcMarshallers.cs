using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Reference-typed wrapper around <see cref="ReplicationBatchEnvelope"/>.
/// gRPC''s <see cref="Method{TRequest, TResponse}"/> imposes a
/// <c>class</c> constraint on its type parameters; the public
/// envelope is a <c>readonly record struct</c> for Orleans-serializer
/// reasons. The wrapper carries the value across the gRPC
/// boundary and is the only allocation the marshaller introduces per
/// call - the encoded payload itself is still written straight into
/// the gRPC stream''s <see cref="System.Buffers.IBufferWriter{T}"/>.
/// </summary>
internal sealed class ReplicationBatchEnvelopeBox
{
    public ReplicationBatchEnvelope Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="ReplicationAck"/>.
/// Mirrors <see cref="ReplicationBatchEnvelopeBox"/>; see that type''s
/// remarks for rationale.
/// </summary>
internal sealed class ReplicationAckBox
{
    public ReplicationAck Value { get; init; }
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
/// to <see cref="IReplicationBatchEncoder.Encode"/>, so the envelope''s
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
    /// <paramref name="encoder"/>.
    /// </summary>
    public static Marshaller<ReplicationBatchEnvelopeBox> CreateEnvelopeMarshaller(IReplicationBatchEncoder encoder)
    {
        ArgumentNullException.ThrowIfNull(encoder);

        return Marshallers.Create<ReplicationBatchEnvelopeBox>(
            serializer: (box, context) =>
            {
                encoder.Encode(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new ReplicationBatchEnvelopeBox { Value = DecodeEnvelope(encoder, context) });
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

    private static ReplicationBatchEnvelope DecodeEnvelope(IReplicationBatchEncoder encoder, GrpcDeserializationContext context)
    {
        var sequence = context.PayloadAsReadOnlySequence();
        if (sequence.IsSingleSegment)
        {
            return encoder.Decode(sequence.First);
        }

        var length = checked((int)sequence.Length);
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            sequence.CopyTo(rented);
            return encoder.Decode(new ReadOnlyMemory<byte>(rented, 0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
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

