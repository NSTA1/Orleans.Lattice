using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Unit coverage for the Orleans-backed gRPC marshaller factory the
/// tenant-administration binding wires onto every RPC. Proves the contextual
/// marshaller round-trips a message through the Orleans serializer and that the
/// deserialize path reassembles the payload correctly whether the gRPC stack hands
/// it a single contiguous segment or a fragmented multi-segment
/// <see cref="ReadOnlySequence{T}"/> - the pooled-copy branch that only a genuinely
/// fragmented payload reaches.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdminGrpcMarshallersTests
{
    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private Serializer<T> Serializer<T>() => _serializers.GetRequiredService<Serializer<T>>();

    private static byte[] SerializeVia<T>(Marshaller<T> marshaller, T value)
    {
        var context = new CapturingSerializationContext();
        marshaller.ContextualSerializer(value, context);
        return context.Written;
    }

    [Test]
    public void Create_throws_on_a_null_serializer()
    {
        Assert.That(
            () => LatticeTenantAdminGrpcMarshallers.Create<TenantAdminCreateRequest>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Serialize_then_deserialize_single_segment_round_trips()
    {
        var marshaller = LatticeTenantAdminGrpcMarshallers.Create(Serializer<TenantAdminCreateRequest>());
        var value = new TenantAdminCreateRequest
        {
            TenantId = "acme",
            AdminSubjects = ["ops@example.com", "sre@example.com"],
        };

        var payload = SerializeVia(marshaller, value);
        var context = new SequenceDeserializationContext(new ReadOnlySequence<byte>(payload));
        var decoded = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TenantId, Is.EqualTo("acme"));
            Assert.That(decoded.AdminSubjects, Is.EqualTo(new[] { "ops@example.com", "sre@example.com" }));
        });
    }

    [Test]
    public void Deserialize_reassembles_a_multi_segment_payload()
    {
        var marshaller = LatticeTenantAdminGrpcMarshallers.Create(Serializer<TenantAdminCreateRequest>());
        var value = new TenantAdminCreateRequest
        {
            TenantId = "a-tenant-id-long-enough-to-span-several-buffer-segments",
            AdminSubjects = ["first@example.com", "second@example.com", "third@example.com"],
        };

        var payload = SerializeVia(marshaller, value);
        var sequence = SegmentedSequence.Of(payload, segments: 4);
        Assert.That(sequence.IsSingleSegment, Is.False,
            "the test payload must be genuinely multi-segment to reach the pooled-copy branch");

        var decoded = marshaller.ContextualDeserializer(new SequenceDeserializationContext(sequence));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TenantId, Is.EqualTo(value.TenantId));
            Assert.That(decoded.AdminSubjects, Is.EqualTo(value.AdminSubjects));
        });
    }

    [Test]
    public void Marshaller_round_trips_a_response_message_too()
    {
        var marshaller = LatticeTenantAdminGrpcMarshallers.Create(Serializer<TenantDeletionResult>());
        var value = new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 7 };

        var payload = SerializeVia(marshaller, value);
        var decoded = marshaller.ContextualDeserializer(
            new SequenceDeserializationContext(new ReadOnlySequence<byte>(payload)));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TenantId, Is.EqualTo("acme"));
            Assert.That(decoded.CascadedTreeCount, Is.EqualTo(7));
        });
    }

    /// <summary>
    /// A <see cref="SerializationContext"/> that captures the encoded payload into
    /// an in-memory buffer, so the marshaller's serialize path can be exercised
    /// without a live gRPC call.
    /// </summary>
    private sealed class CapturingSerializationContext : SerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public byte[] Written => _writer.WrittenSpan.ToArray();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void SetPayloadLength(int payloadLength)
        {
        }

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override void Complete()
        {
        }
    }

    /// <summary>
    /// A <see cref="DeserializationContext"/> that replays a caller-supplied
    /// <see cref="ReadOnlySequence{T}"/>, so the marshaller's single-segment and
    /// multi-segment branches can each be driven deterministically.
    /// </summary>
    private sealed class SequenceDeserializationContext(ReadOnlySequence<byte> payload)
        : global::Grpc.Core.DeserializationContext
    {
        public override int PayloadLength => checked((int)payload.Length);

        public override byte[] PayloadAsNewBuffer() => payload.ToArray();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => payload;
    }

    /// <summary>
    /// Splits a contiguous buffer into linked segments so the resulting sequence is
    /// genuinely multi-segment.
    /// </summary>
    private static class SegmentedSequence
    {
        public static ReadOnlySequence<byte> Of(byte[] payload, int segments)
        {
            if (payload.Length < segments)
            {
                segments = Math.Max(1, payload.Length);
            }

            var chunkSize = Math.Max(1, payload.Length / segments);
            Segment? head = null;
            Segment? tail = null;
            for (var offset = 0; offset < payload.Length; offset += chunkSize)
            {
                var length = Math.Min(chunkSize, payload.Length - offset);
                var slice = new ReadOnlyMemory<byte>(payload, offset, length);
                if (head is null)
                {
                    head = new Segment(slice, 0);
                    tail = head;
                }
                else
                {
                    tail = tail!.Append(slice);
                }
            }

            head ??= new Segment(ReadOnlyMemory<byte>.Empty, 0);
            tail ??= head;
            return new ReadOnlySequence<byte>(head, 0, tail, tail.Memory.Length);
        }

        private sealed class Segment : ReadOnlySequenceSegment<byte>
        {
            public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
            {
                Memory = memory;
                RunningIndex = runningIndex;
            }

            public Segment Append(ReadOnlyMemory<byte> memory)
            {
                var next = new Segment(memory, RunningIndex + Memory.Length);
                Next = next;
                return next;
            }
        }
    }
}
