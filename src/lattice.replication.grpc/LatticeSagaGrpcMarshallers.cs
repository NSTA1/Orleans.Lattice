using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Reference-typed wrapper around <see cref="SagaControlRequest"/>.
/// gRPC's <see cref="Method{TRequest, TResponse}"/> imposes a
/// <c>class</c> constraint on its type parameters; the public DTO is a
/// <c>readonly record struct</c>. The wrapper carries the value across
/// the gRPC boundary and is the only allocation the marshaller
/// introduces per call.
/// </summary>
internal sealed class SagaControlRequestBox
{
    /// <summary>The wrapped saga control request value.</summary>
    public SagaControlRequest Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around <see cref="SagaControlResponse"/>.
/// See <see cref="SagaControlRequestBox"/> for the gRPC reference-type
/// constraint rationale.
/// </summary>
internal sealed class SagaControlResponseBox
{
    /// <summary>The wrapped saga control response value.</summary>
    public SagaControlResponse Value { get; init; }
}

/// <summary>
/// Builds gRPC <see cref="Marshaller{T}"/> instances for the saga
/// control RPCs. Every marshaller delegates to the Orleans binary
/// serialiser and hands the gRPC
/// <see cref="SerializationContext.GetBufferWriter"/> straight to the
/// serialiser so the encoded payload never round-trips through a
/// per-call managed buffer. Multi-segment deserialisation falls back
/// to a pooled <see cref="ArrayPool{T}"/> buffer.
/// </summary>
internal static class LatticeSagaGrpcMarshallers
{
    /// <summary>
    /// Builds a marshaller for the <see cref="SagaControlRequestBox"/>
    /// request payload shared by all four saga control RPCs.
    /// </summary>
    public static Marshaller<SagaControlRequestBox> CreateRequestMarshaller(
        Serializer<SagaControlRequest> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<SagaControlRequestBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new SagaControlRequestBox { Value = DeserializeRequest(serializer, context) });
    }

    /// <summary>
    /// Builds a marshaller for the <see cref="SagaControlResponseBox"/>
    /// response payload shared by all four saga control RPCs.
    /// </summary>
    public static Marshaller<SagaControlResponseBox> CreateResponseMarshaller(
        Serializer<SagaControlResponse> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<SagaControlResponseBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new SagaControlResponseBox { Value = DeserializeResponse(serializer, context) });
    }

    private static SagaControlRequest DeserializeRequest(
        Serializer<SagaControlRequest> serializer,
        GrpcDeserializationContext context)
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

    private static SagaControlResponse DeserializeResponse(
        Serializer<SagaControlResponse> serializer,
        GrpcDeserializationContext context)
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
