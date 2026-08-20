using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Direct unit tests for the hand-written <see cref="LatticeSchemaGrpcMarshallers"/>
/// serializer/deserializer hand-off, driving both the single-segment fast path and
/// the multi-segment rented-buffer path of the deserializer through a fake
/// <see cref="Grpc.Core.DeserializationContext"/> - with no gRPC stream. The single-segment
/// path is also exercised indirectly by the client/service fixtures; this fixture
/// pins the multi-segment branch and the null guard deterministically.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcMarshallersUnitTests
{
    private ServiceProvider _serializerProvider = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _serializerProvider.Dispose();

    private Serializer<SchemaTreeRequest> RequestSerializer() =>
        _serializerProvider.GetRequiredService<Serializer<SchemaTreeRequest>>();

    private static byte[] Encode(Serializer<SchemaTreeRequest> serializer, SchemaTreeRequest value)
    {
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        return writer.WrittenSpan.ToArray();
    }

    [Test]
    public void Create_with_null_serializer_throws()
    {
        Assert.That(
            () => LatticeSchemaGrpcMarshallers.Create<SchemaTreeRequest>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Deserialize_from_a_single_segment_payload_round_trips()
    {
        var serializer = RequestSerializer();
        var marshaller = LatticeSchemaGrpcMarshallers.Create(serializer);
        var bytes = Encode(serializer, new SchemaTreeRequest { TreeId = "orders" });
        var context = new FakeDeserializationContext(new ReadOnlySequence<byte>(bytes));

        var result = marshaller.ContextualDeserializer(context);

        Assert.That(result.TreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void Deserialize_from_a_multi_segment_payload_round_trips()
    {
        var serializer = RequestSerializer();
        var marshaller = LatticeSchemaGrpcMarshallers.Create(serializer);
        var bytes = Encode(serializer, new SchemaTreeRequest { TreeId = "inventory" });
        var context = new FakeDeserializationContext(SplitIntoTwoSegments(bytes));

        var result = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(context.SequenceWasMultiSegment, Is.True);
            Assert.That(result.TreeId, Is.EqualTo("inventory"));
        });
    }

    private static ReadOnlySequence<byte> SplitIntoTwoSegments(byte[] bytes)
    {
        var mid = bytes.Length / 2;
        var first = new MemorySegment<byte>(bytes.AsMemory(0, mid));
        var second = first.Append(bytes.AsMemory(mid));
        return new ReadOnlySequence<byte>(first, 0, second, second.Memory.Length);
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

    private sealed class FakeDeserializationContext(ReadOnlySequence<byte> payload) : global::Grpc.Core.DeserializationContext
    {
        private readonly ReadOnlySequence<byte> _payload = payload;

        public bool SequenceWasMultiSegment { get; private set; }

        public override int PayloadLength => checked((int)_payload.Length);

        public override byte[] PayloadAsNewBuffer() => _payload.ToArray();

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence()
        {
            SequenceWasMultiSegment = !_payload.IsSingleSegment;
            return _payload;
        }
    }
}
