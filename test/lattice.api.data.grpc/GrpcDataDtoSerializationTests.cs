using System.Buffers;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using GrpcSerializationContext = Grpc.Core.SerializationContext;
using GrpcDeserializationContext = Grpc.Core.DeserializationContext;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire DTOs (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent and stable across the wire, and round-trips a
/// message through the actual <see cref="LatticeDataApiGrpcMarshallers"/> to
/// prove the contextual serialize / deserialize hand-off the gRPC stream uses is
/// byte-faithful. The transport-agnostic facade DTOs (the Get response and the
/// range request / page) are covered in the <c>Orleans.Lattice.Api.Data</c> test
/// project.
/// </summary>
[TestFixture]
public sealed class GrpcDataDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    private T MarshalRoundTrip<T>(T value)
        where T : class
    {
        var marshaller = LatticeDataApiGrpcMarshallers.Create(_services.GetRequiredService<Serializer<T>>());

        var serializationContext = new FakeSerializationContext();
        marshaller.ContextualSerializer(value, serializationContext);

        var deserializationContext = new FakeDeserializationContext(serializationContext.ToArray());
        return marshaller.ContextualDeserializer(deserializationContext);
    }

    [Test]
    public void DataSetRequest_round_trips()
    {
        var original = new DataSetRequest { TreeId = "tree-a", Key = "k1", Value = new byte[] { 1, 2, 3 } };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Key, Is.EqualTo("k1"));
            Assert.That(copy.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void DataSetResponse_round_trips()
    {
        Assert.That(RoundTrip(new DataSetResponse()), Is.EqualTo(new DataSetResponse()));
    }

    [Test]
    public void DataDeleteRequest_round_trips()
    {
        var original = new DataDeleteRequest { TreeId = "tree-a", Key = "k1" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void DataDeleteResponse_round_trips()
    {
        var copy = RoundTrip(new DataDeleteResponse { Removed = true });

        Assert.That(copy.Removed, Is.True);
    }

    [Test]
    public void DataAtomicRequest_round_trips_with_batch()
    {
        var original = new DataAtomicRequest
        {
            TreeId = "tree-a",
            OperationId = "op-1",
            Batch = new DataAtomicBatch
            {
                Upserts = [new DataEntry { Key = "a", Value = new byte[] { 9 } }],
                DeleteKeys = ["b"],
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.OperationId, Is.EqualTo("op-1"));
            Assert.That(copy.Batch.Upserts.Select(e => e.Key), Is.EqualTo(new[] { "a" }));
            Assert.That(copy.Batch.DeleteKeys, Is.EqualTo(new[] { "b" }));
        });
    }

    [Test]
    public void DataAtomicResponse_round_trips()
    {
        Assert.That(RoundTrip(new DataAtomicResponse()), Is.EqualTo(new DataAtomicResponse()));
    }

    [Test]
    public void DataCrossTreeRequest_round_trips_with_batches()
    {
        var original = new DataCrossTreeRequest
        {
            OperationId = "xt-1",
            Batches =
            [
                new DataTreeBatch { TreeId = "a", Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
                new DataTreeBatch { TreeId = "b", DeleteKeys = ["z"] },
            ],
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.OperationId, Is.EqualTo("xt-1"));
            Assert.That(copy.Batches.Select(b => b.TreeId), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(copy.Batches[1].DeleteKeys, Is.EqualTo(new[] { "z" }));
        });
    }

    [Test]
    public void DataCrossTreeResponse_round_trips_each_outcome()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RoundTrip(new DataCrossTreeResponse { Outcome = CrossTreeAtomicWriteOutcome.Committed }).Outcome,
                Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(
                RoundTrip(new DataCrossTreeResponse { Outcome = CrossTreeAtomicWriteOutcome.PreconditionFailed }).Outcome,
                Is.EqualTo(CrossTreeAtomicWriteOutcome.PreconditionFailed));
        });
    }

    [Test]
    public void DataGetRequest_round_trips()
    {
        var original = new DataGetRequest { TreeId = "tree-a", Key = "k1" };

        Assert.That(RoundTrip(original), Is.EqualTo(original));
    }

    [Test]
    public void Marshaller_round_trips_a_set_request_through_the_grpc_contexts()
    {
        var original = new DataSetRequest { TreeId = "tree-a", Key = "k1", Value = new byte[] { 7, 7, 7 } };

        var copy = MarshalRoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-a"));
            Assert.That(copy.Key, Is.EqualTo("k1"));
            Assert.That(copy.Value, Is.EqualTo(new byte[] { 7, 7, 7 }));
        });
    }

    [Test]
    public void Marshaller_round_trips_a_read_result_through_the_grpc_contexts()
    {
        var original = new DataReadResult { TreeId = "t", Key = "k", Found = true, Value = new byte[] { 5, 6 } };

        var copy = MarshalRoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.True);
            Assert.That(copy.Value, Is.EqualTo(new byte[] { 5, 6 }));
        });
    }

    /// <summary>
    /// Minimal <see cref="GrpcSerializationContext"/> that captures the encoded
    /// bytes written through the buffer-writer hand-off the marshaller uses.
    /// </summary>
    private sealed class FakeSerializationContext : GrpcSerializationContext
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

    /// <summary>
    /// Minimal <see cref="GrpcDeserializationContext"/> that presents a fixed
    /// payload as the read-only sequence the marshaller decodes.
    /// </summary>
    private sealed class FakeDeserializationContext(byte[] payload) : GrpcDeserializationContext
    {
        public override int PayloadLength => payload.Length;

        public override ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(payload);

        public override byte[] PayloadAsNewBuffer() => (byte[])payload.Clone();
    }
}
