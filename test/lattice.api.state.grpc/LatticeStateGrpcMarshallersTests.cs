using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;
using GrpcSerializationContext = Grpc.Core.SerializationContext;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeStateGrpcMarshallers"/>, exercising both
/// the single-segment and multi-segment deserialize paths of the Orleans-backed
/// gRPC marshaller in-process with fake serialization contexts. The
/// multi-segment path (the array-pool copy) is not reachable over the real
/// transport in the integration tests, which typically hand a single contiguous
/// buffer, so it is proven here directly.
/// </summary>
[TestFixture]
public sealed class LatticeStateGrpcMarshallersTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void Create_when_serializer_is_null_throws()
    {
        Assert.That(
            () => LatticeStateGrpcMarshallers.Create<ClusterInfo>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Marshaller_round_trips_single_segment_payload()
    {
        var serializer = _services.GetRequiredService<Serializer<ClusterInfo>>();
        var marshaller = LatticeStateGrpcMarshallers.Create(serializer);
        var original = new ClusterInfo { ClusterId = "single-seg", ServiceId = "svc" };

        var payload = Serialize(marshaller, original);
        var context = new FakeDeserializationContext(new ReadOnlySequence<byte>(payload));

        var copy = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ClusterId, Is.EqualTo("single-seg"));
            Assert.That(copy.ServiceId, Is.EqualTo("svc"));
        });
    }

    [Test]
    public void Marshaller_round_trips_multi_segment_payload()
    {
        var serializer = _services.GetRequiredService<Serializer<ClusterInfo>>();
        var marshaller = LatticeStateGrpcMarshallers.Create(serializer);
        var original = new ClusterInfo { ClusterId = "multi-seg-cluster", ServiceId = "multi-seg-service" };

        var payload = Serialize(marshaller, original);
        var multiSegment = BuildMultiSegmentSequence(payload, chunkSize: 3);
        Assert.That(multiSegment.IsSingleSegment, Is.False, "the fabricated payload must span multiple segments");

        var context = new FakeDeserializationContext(multiSegment);
        var copy = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(copy.ClusterId, Is.EqualTo("multi-seg-cluster"));
            Assert.That(copy.ServiceId, Is.EqualTo("multi-seg-service"));
        });
    }

    private static byte[] Serialize<T>(Marshaller<T> marshaller, T value)
    {
        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(value, context);
        return context.ToArray();
    }

    private static ReadOnlySequence<byte> BuildMultiSegmentSequence(byte[] data, int chunkSize)
    {
        BufferSegment? first = null;
        BufferSegment? last = null;
        for (var offset = 0; offset < data.Length; offset += chunkSize)
        {
            var length = Math.Min(chunkSize, data.Length - offset);
            var memory = new ReadOnlyMemory<byte>(data, offset, length);
            if (first is null)
            {
                first = new BufferSegment(memory, runningIndex: 0);
                last = first;
            }
            else
            {
                last = last!.Append(memory);
            }
        }

        // A zero-length payload cannot be multi-segment; the tests always pass
        // real serialized data, so first/last are non-null here.
        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public BufferSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new BufferSegment(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }

    private sealed class FakeSerializationContext : GrpcSerializationContext
    {
        private readonly ArrayBufferWriter<byte> _writer = new();

        public override void Complete(byte[] payload) => _writer.Write(payload);

        public override IBufferWriter<byte> GetBufferWriter() => _writer;

        public override void Complete()
        {
            // The Orleans marshaller writes straight into the buffer writer and
            // then signals completion; nothing further is required here.
        }

        public byte[] ToArray() => _writer.WrittenSpan.ToArray();
    }

    private sealed class FakeDeserializationContext : GrpcDeserializationContext
    {
        private readonly ReadOnlySequence<byte> _payload;

        public FakeDeserializationContext(ReadOnlySequence<byte> payload) => _payload = payload;

        public override int PayloadLength => checked((int)_payload.Length);

        public override byte[] PayloadAsNewBuffer() => _payload.ToArray();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => _payload;
    }
}
