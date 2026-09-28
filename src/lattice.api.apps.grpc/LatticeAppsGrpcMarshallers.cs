using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal static partial class LatticeAppsGrpcMarshallers
{
    public static Marshaller<T> Create<T>(Serializer<T> serializer) where T : class
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return Marshallers.Create<T>(
            (value, context) =>
            {
                serializer.Serialize(value, context.GetBufferWriter());
                context.Complete();
            },
            context =>
            {
                var sequence = context.PayloadAsReadOnlySequence();
                if (sequence.IsSingleSegment)
                    return serializer.Deserialize(sequence.First.Span);

                var length = checked((int)sequence.Length);
                var buffer = ArrayPool<byte>.Shared.Rent(length);
                try
                {
                    sequence.CopyTo(buffer);
                    return serializer.Deserialize(buffer.AsSpan(0, length));
                }
                finally
                {
                    // Install/consent payloads can contain membership and policy data.
                    ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
                }
            });
    }
}
