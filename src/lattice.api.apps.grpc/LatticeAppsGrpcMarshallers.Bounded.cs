using System.Buffers;
using Grpc.Core;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal static partial class LatticeAppsGrpcMarshallers
{
    /// <summary>
    /// The per-method message bound of the asset-carrying RPCs: the 2 MiB per-asset byte cap of an app bundle
    /// plus a small allowance for the path, media type, digest and framing that travel with the bytes. It is
    /// applied to those methods only, in both directions, never as a raise of the service or channel limit.
    /// </summary>
    public const int MaxAssetMessageBytes = (2 * 1024 * 1024) + (4 * 1024);

    /// <summary>
    /// Creates a marshaller that refuses to serialize or deserialize a message larger than
    /// <paramref name="maxMessageBytes"/>, failing the call with <see cref="StatusCode.ResourceExhausted"/>.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="serializer">The Orleans serializer.</param>
    /// <param name="maxMessageBytes">The largest message accepted, in bytes.</param>
    /// <returns>The bounded marshaller.</returns>
    public static Marshaller<T> CreateBounded<T>(Serializer<T> serializer, int maxMessageBytes) where T : class
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessageBytes);
        return Marshallers.Create<T>(
            (value, context) =>
            {
                var writer = new BoundedBufferWriter(context.GetBufferWriter(), maxMessageBytes);
                serializer.Serialize(value, writer);
                context.Complete();
            },
            context =>
            {
                var sequence = context.PayloadAsReadOnlySequence();
                if (sequence.Length > maxMessageBytes)
                    throw TooLarge();
                if (sequence.IsSingleSegment)
                    return serializer.Deserialize(sequence.First.Span);

                var length = (int)sequence.Length;
                var buffer = ArrayPool<byte>.Shared.Rent(length);
                try
                {
                    sequence.CopyTo(buffer);
                    return serializer.Deserialize(buffer.AsSpan(0, length));
                }
                finally
                {
                    // Clear only the copied prefix, not the whole rounded-up
                    // rental: see the unbounded marshaller for the arithmetic.
                    buffer.AsSpan(0, length).Clear();
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            });
    }

    private static RpcException TooLarge() =>
        new(new Status(StatusCode.ResourceExhausted, "The app asset message exceeds the per-method size bound."));

    /// <summary>Forwards to the transport's buffer writer and fails once more than the bound has been written.</summary>
    private sealed class BoundedBufferWriter(IBufferWriter<byte> inner, int limit) : IBufferWriter<byte>
    {
        private long _written;

        public void Advance(int count)
        {
            _written += count;
            if (_written > limit)
                throw TooLarge();
            inner.Advance(count);
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);

        public Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);
    }
}
