using System.Buffers;
using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Test helper that pushes a message through a gRPC <see cref="Marshaller{T}"/>
/// exactly as the transport does: serialize into a buffer writer via the
/// contextual serializer, then decode the captured bytes via the contextual
/// deserializer. Used by the loopback round-trip to prove the new directory /
/// access-model RPCs cross the real wire marshallers byte-faithfully rather than
/// hand the same object reference to both ends.
/// </summary>
internal static class GrpcWireRoundTrip
{
    public static T Through<T>(Marshaller<T> marshaller, T value)
    {
        var serialization = new BufferSerializationContext();
        marshaller.ContextualSerializer(value, serialization);

        var deserialization = new BytesDeserializationContext(serialization.ToArray());
        return marshaller.ContextualDeserializer(deserialization);
    }

    private sealed class BufferSerializationContext : SerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete()
        {
        }

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public byte[] ToArray() => _writer.WrittenSpan.ToArray();
    }

    private sealed class BytesDeserializationContext(byte[] payload) : DeserializationContext
    {
        public override int PayloadLength => payload.Length;

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(payload);

        public override byte[] PayloadAsNewBuffer() => (byte[])payload.Clone();
    }
}
