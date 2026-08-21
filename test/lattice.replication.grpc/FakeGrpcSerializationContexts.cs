using System.Buffers;
using Grpc.Core;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// In-process test doubles for the gRPC contextual serialization API.
/// The production marshallers hand the gRPC
/// <see cref="SerializationContext.GetBufferWriter"/> straight to the
/// Orleans serializer and read payloads back through
/// <see cref="DeserializationContext.PayloadAsReadOnlySequence"/>; these
/// fakes let the marshaller delegates be driven directly - without
/// standing up a gRPC channel or server - so both the single-segment
/// fast path and the multi-segment pooled-buffer fallback are covered.
/// </summary>
internal static class FakeGrpcSerializationContexts
{
    /// <summary>
    /// Captures the bytes a marshaller serializer writes into the
    /// gRPC buffer writer and records whether <c>Complete()</c> was
    /// called.
    /// </summary>
    internal sealed class RecordingSerializationContext : SerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public bool Completed { get; private set; }

        public byte[] WrittenBytes => _writer.WrittenSpan.ToArray();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete() => Completed = true;

        public override void Complete(byte[] payload)
        {
            _writer.Write(payload);
            Completed = true;
        }

        public override void SetPayloadLength(int payloadLength)
        {
            // No-op: the ArrayBufferWriter tracks its own length.
        }
    }

    /// <summary>
    /// Serves a payload back to a marshaller deserializer as a
    /// single-segment <see cref="ReadOnlySequence{T}"/>.
    /// </summary>
    internal sealed class SingleSegmentDeserializationContext : DeserializationContext
    {
        private readonly byte[] _payload;

        public SingleSegmentDeserializationContext(byte[] payload) => _payload = payload;

        public override int PayloadLength => _payload.Length;

        public override byte[] PayloadAsNewBuffer() => (byte[])_payload.Clone();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence()
            => new(_payload);
    }

    /// <summary>
    /// Serves a payload back to a marshaller deserializer as a
    /// multi-segment <see cref="ReadOnlySequence{T}"/> split at the
    /// supplied boundary, forcing the pooled-buffer fallback path.
    /// </summary>
    internal sealed class MultiSegmentDeserializationContext : DeserializationContext
    {
        private readonly byte[] _payload;
        private readonly ReadOnlySequence<byte> _sequence;

        public MultiSegmentDeserializationContext(byte[] payload, int splitAt)
        {
            _payload = payload;
            _sequence = BuildMultiSegment(payload, splitAt);
        }

        public override int PayloadLength => _payload.Length;

        public override byte[] PayloadAsNewBuffer() => (byte[])_payload.Clone();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _sequence;
    }

    /// <summary>
    /// Builds a two-segment <see cref="ReadOnlySequence{T}"/> over a
    /// copy of <paramref name="payload"/>, split at
    /// <paramref name="splitAt"/>, so <c>IsSingleSegment</c> is
    /// <see langword="false"/>.
    /// </summary>
    internal static ReadOnlySequence<byte> BuildMultiSegment(byte[] payload, int splitAt)
    {
        if (splitAt <= 0 || splitAt >= payload.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(splitAt),
                splitAt,
                "splitAt must be strictly inside the payload so two non-empty segments result.");
        }

        var first = new MemorySegment<byte>(payload.AsMemory(0, splitAt));
        var second = first.Append(payload.AsMemory(splitAt));
        return new ReadOnlySequence<byte>(first, 0, second, second.Memory.Length);
    }

    private sealed class MemorySegment<T> : ReadOnlySequenceSegment<T>
    {
        public MemorySegment(ReadOnlyMemory<T> memory) => Memory = memory;

        public MemorySegment<T> Append(ReadOnlyMemory<T> memory)
        {
            var segment = new MemorySegment<T>(memory)
            {
                RunningIndex = RunningIndex + Memory.Length,
            };
            Next = segment;
            return segment;
        }
    }
}
