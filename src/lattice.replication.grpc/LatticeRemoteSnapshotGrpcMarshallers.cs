using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// Reference-typed wrapper around
/// <see cref="RemoteSnapshotMetadataRequest"/>. gRPC's
/// <see cref="Method{TRequest, TResponse}"/> imposes a <c>class</c>
/// constraint on its type parameters; the public DTO is a
/// <c>readonly record struct</c>. The wrapper carries the value across
/// the gRPC boundary and is the only allocation the marshaller
/// introduces per call.
/// </summary>
internal sealed class RemoteSnapshotMetadataRequestBox
{
    public RemoteSnapshotMetadataRequest Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around
/// <see cref="RemoteSnapshotMetadata"/>. See
/// <see cref="RemoteSnapshotMetadataRequestBox"/> for the gRPC
/// reference-type constraint rationale.
/// </summary>
internal sealed class RemoteSnapshotMetadataBox
{
    public RemoteSnapshotMetadata Value { get; init; }
}

/// <summary>
/// Reference-typed wrapper around
/// <see cref="RemoteSnapshotStreamItem"/>. See
/// <see cref="RemoteSnapshotMetadataRequestBox"/> for the gRPC
/// reference-type constraint rationale.
/// </summary>
internal sealed class RemoteSnapshotStreamItemBox
{
    public RemoteSnapshotStreamItem Value { get; init; }
}

/// <summary>
/// Builds gRPC <see cref="Marshaller{T}"/> instances for the snapshot
/// transport RPCs. Every marshaller delegates to the Orleans binary
/// serialiser and hands the gRPC
/// <see cref="SerializationContext.GetBufferWriter"/> straight to the
/// serialiser so the encoded payload never round-trips through a
/// per-call managed buffer. Multi-segment deserialisation falls back
/// to a pooled <see cref="ArrayPool{T}"/> buffer.
/// </summary>
internal static class LatticeRemoteSnapshotGrpcMarshallers
{
    /// <summary>
    /// Builds a marshaller for the
    /// <see cref="RemoteSnapshotMetadataRequestBox"/> request payload
    /// shared by both snapshot RPCs.
    /// </summary>
    public static Marshaller<RemoteSnapshotMetadataRequestBox> CreateRequestMarshaller(
        Serializer<RemoteSnapshotMetadataRequest> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<RemoteSnapshotMetadataRequestBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new RemoteSnapshotMetadataRequestBox { Value = DeserializeRequest(serializer, context) });
    }

    /// <summary>
    /// Builds a marshaller for the
    /// <see cref="RemoteSnapshotMetadataBox"/> response payload of the
    /// unary <c>GetMetadata</c> RPC.
    /// </summary>
    public static Marshaller<RemoteSnapshotMetadataBox> CreateMetadataMarshaller(
        Serializer<RemoteSnapshotMetadata> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<RemoteSnapshotMetadataBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new RemoteSnapshotMetadataBox { Value = DeserializeMetadata(serializer, context) });
    }

    /// <summary>
    /// Builds a marshaller for the
    /// <see cref="RemoteSnapshotStreamItemBox"/> response payload of
    /// the server-streaming <c>RequestSnapshot</c> RPC.
    /// </summary>
    public static Marshaller<RemoteSnapshotStreamItemBox> CreateStreamItemMarshaller(
        Serializer<RemoteSnapshotStreamItem> serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<RemoteSnapshotStreamItemBox>(
            serializer: (box, context) =>
            {
                serializer.Serialize(box.Value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => new RemoteSnapshotStreamItemBox { Value = DeserializeStreamItem(serializer, context) });
    }

    private static RemoteSnapshotMetadataRequest DeserializeRequest(
        Serializer<RemoteSnapshotMetadataRequest> serializer,
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

    private static RemoteSnapshotMetadata DeserializeMetadata(
        Serializer<RemoteSnapshotMetadata> serializer,
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

    private static RemoteSnapshotStreamItem DeserializeStreamItem(
        Serializer<RemoteSnapshotStreamItem> serializer,
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