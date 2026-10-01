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
                    // Install/consent payloads can contain membership and policy
                    // data, so the bytes this call copied in are cleared. Only
                    // that prefix: clearArray: true memsets the whole rounded-up
                    // rental, which Rent may have sized at nearly twice the
                    // payload, and this runs on every multi-segment RPC.
                    buffer.AsSpan(0, length).Clear();
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            });
    }
}
