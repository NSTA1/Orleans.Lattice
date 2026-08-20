using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using GrpcSerializationContext = Grpc.Core.SerializationContext;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationGrpcMarshallers"/>: the contextual
/// marshaller serializes a message into a gRPC buffer writer and deserializes it
/// back from both the single-segment and the multi-segment
/// <see cref="ReadOnlySequence{T}"/> the gRPC payload can present. Uses in-memory
/// serialization/deserialization context fakes so no gRPC transport is involved.
/// </summary>
public sealed class LatticeReplicationGrpcMarshallersTests
{
    private ServiceProvider _services = null!;
    private Marshaller<ReplicationEnableRequestMessage> _marshaller = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _marshaller = LatticeReplicationGrpcMarshallers.Create(
            _services.GetRequiredService<Serializer<ReplicationEnableRequestMessage>>());
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Create_null_serializer_throws() =>
        Assert.Throws<ArgumentNullException>(
            () => LatticeReplicationGrpcMarshallers.Create<ReplicationEnableRequestMessage>(null!));

    [Test]
    public void ContextualSerializer_and_single_segment_deserializer_round_trip()
    {
        var message = new ReplicationEnableRequestMessage
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.RwFlag,
            BootstrapSourceClusterId = "cluster-b",
        };

        var payload = Serialize(message);
        var context = new FakeDeserializationContext(new ReadOnlySequence<byte>(payload));

        var decoded = _marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo("orders"));
            Assert.That(decoded.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(decoded.BootstrapSourceClusterId, Is.EqualTo("cluster-b"));
        });
    }

    [Test]
    public void ContextualDeserializer_reads_a_multi_segment_payload()
    {
        var message = new ReplicationEnableRequestMessage
        {
            TreeId = "customers",
            Mode = LatticeMergeMode.LwwRegister,
            BootstrapSourceClusterId = "cluster-c",
        };

        var payload = Serialize(message);
        Assume.That(payload.Length, Is.GreaterThan(1), "payload must be splittable into two segments");
        var context = new FakeDeserializationContext(BuildMultiSegment(payload));

        var decoded = _marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo("customers"));
            Assert.That(decoded.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(decoded.BootstrapSourceClusterId, Is.EqualTo("cluster-c"));
        });
    }

    private byte[] Serialize(ReplicationEnableRequestMessage message)
    {
        var context = new FakeSerializationContext();
        _marshaller.ContextualSerializer(message, context);

        Assert.That(context.Completed, Is.True);
        return context.Payload;
    }

    private static ReadOnlySequence<byte> BuildMultiSegment(byte[] data)
    {
        var mid = data.Length / 2;
        var first = new MemorySegment<byte>(data.AsMemory(0, mid));
        var last = first.Append(data.AsMemory(mid));
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private sealed class FakeSerializationContext : GrpcSerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public bool Completed { get; private set; }

        public byte[] Payload => _writer.WrittenSpan.ToArray();

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete() => Completed = true;

        public override void Complete(byte[] payload)
        {
            _writer.Write(payload);
            Completed = true;
        }

        public override void SetPayloadLength(int payloadLength)
        {
        }
    }

    private sealed class FakeDeserializationContext : GrpcDeserializationContext
    {
        private readonly ReadOnlySequence<byte> _sequence;

        public FakeDeserializationContext(ReadOnlySequence<byte> sequence) => _sequence = sequence;

        public override int PayloadLength => checked((int)_sequence.Length);

        public override byte[] PayloadAsNewBuffer() => _sequence.ToArray();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _sequence;
    }

    private sealed class MemorySegment<T> : ReadOnlySequenceSegment<T>
    {
        public MemorySegment(ReadOnlyMemory<T> memory) => Memory = memory;

        public MemorySegment<T> Append(ReadOnlyMemory<T> memory)
        {
            var segment = new MemorySegment<T>(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = segment;
            return segment;
        }
    }
}
