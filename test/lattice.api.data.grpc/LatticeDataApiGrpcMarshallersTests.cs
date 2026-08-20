using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for the Orleans-backed gRPC marshaller factory. Proves the
/// contextual marshaller round-trips a message through the Orleans serializer and
/// that the deserialize path reassembles the payload correctly whether the gRPC
/// stack hands it a single contiguous segment or a fragmented multi-segment
/// <see cref="System.Buffers.ReadOnlySequence{T}"/>.
/// </summary>
[TestFixture]
public sealed class LatticeDataApiGrpcMarshallersTests
{
    private static Serializer<DataSetRequest> Serializer()
    {
        var provider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        return provider.GetRequiredService<Serializer<DataSetRequest>>();
    }

    private static byte[] SerializeVia(Marshaller<DataSetRequest> marshaller, DataSetRequest value)
    {
        var context = new CapturingSerializationContext();
        marshaller.ContextualSerializer(value, context);
        return context.Written;
    }

    [Test]
    public void Create_throws_on_null_serializer()
    {
        Assert.Throws<ArgumentNullException>(
            () => LatticeDataApiGrpcMarshallers.Create<DataSetRequest>(null!));
    }

    [Test]
    public void Serialize_then_deserialize_single_segment_round_trips()
    {
        var marshaller = LatticeDataApiGrpcMarshallers.Create(Serializer());
        var value = new DataSetRequest { TreeId = "tree-a", Key = "k1", Value = [1, 2, 3, 4] };

        var payload = SerializeVia(marshaller, value);
        var context = new SequenceDeserializationContext(ReadOnlySequenceFactory.Single(payload));
        var decoded = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo("tree-a"));
            Assert.That(decoded.Key, Is.EqualTo("k1"));
            Assert.That(decoded.Value, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
        });
    }

    [Test]
    public void Deserialize_reassembles_a_multi_segment_payload()
    {
        var marshaller = LatticeDataApiGrpcMarshallers.Create(Serializer());
        var value = new DataSetRequest
        {
            TreeId = "tree-with-a-longer-id",
            Key = "some-key-that-spans-segments",
            Value = [10, 20, 30, 40, 50, 60, 70, 80, 90],
        };

        var payload = SerializeVia(marshaller, value);
        var sequence = ReadOnlySequenceFactory.Multi(payload, segments: 4);
        Assert.That(sequence.IsSingleSegment, Is.False, "the test payload must be genuinely multi-segment");

        var context = new SequenceDeserializationContext(sequence);
        var decoded = marshaller.ContextualDeserializer(context);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeId, Is.EqualTo(value.TreeId));
            Assert.That(decoded.Key, Is.EqualTo(value.Key));
            Assert.That(decoded.Value, Is.EqualTo(value.Value));
        });
    }
}
