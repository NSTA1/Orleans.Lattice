using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Builds gRPC <see cref="Marshaller{T}"/> instances that delegate to the
/// Orleans serializer, so every backup control-API wire message rides the same
/// versioned, additive-only serialization contract the facade DTOs already
/// use. The buffer-writer hand-off writes the encoded payload straight into
/// the gRPC stream's <see cref="IBufferWriter{T}"/> without an intermediate
/// array.
/// </summary>
internal static class LatticeBackupGrpcMarshallers
{
    /// <summary>
    /// Builds a contextual <see cref="Marshaller{T}"/> for <typeparamref name="T"/>
    /// bound to the supplied Orleans <paramref name="serializer"/>.
    /// </summary>
    public static Marshaller<T> Create<T>(Serializer<T> serializer)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(serializer);

        return Marshallers.Create<T>(
            serializer: (value, context) =>
            {
                serializer.Serialize(value, context.GetBufferWriter());
                context.Complete();
            },
            deserializer: context => Deserialize(serializer, context));
    }

    private static T Deserialize<T>(Serializer<T> serializer, GrpcDeserializationContext context)
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
