using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;
using static Orleans.Lattice.Replication.Grpc.Tests.FakeGrpcSerializationContexts;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Drives the saga and remote-snapshot gRPC marshaller contextual
/// serializer/deserializer delegates directly via in-process fake gRPC
/// contexts, covering the buffer-writer serialize hand-off plus the
/// single-segment fast path and the multi-segment pooled fallback for
/// each marshaller.
/// </summary>
[TestFixture]
public class LatticeSagaAndSnapshotGrpcMarshallersContextTests
{
    private ServiceProvider _sp = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => _sp = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _sp.Dispose();

    private Serializer<T> Ser<T>() => _sp.GetRequiredService<Serializer<T>>();

    private static TBox RoundTrip<TBox>(global::Grpc.Core.Marshaller<TBox> marshaller, TBox box, bool multiSegment)
    {
        var serializationContext = new RecordingSerializationContext();
        marshaller.ContextualSerializer(box, serializationContext);
        Assert.That(serializationContext.Completed, Is.True);

        var bytes = serializationContext.WrittenBytes;
        global::Grpc.Core.DeserializationContext deserializationContext =
            multiSegment && bytes.Length >= 2
                ? new MultiSegmentDeserializationContext(bytes, bytes.Length / 2)
                : new SingleSegmentDeserializationContext(bytes);

        return marshaller.ContextualDeserializer(deserializationContext);
    }

    [Test]
    public void SagaRequestMarshaller_single_segment_round_trip_succeeds()
    {
        var marshaller = LatticeSagaGrpcMarshallers.CreateRequestMarshaller(Ser<SagaControlRequest>());

        var decoded = RoundTrip(marshaller, new SagaControlRequestBox { Value = default }, multiSegment: false);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SagaRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeSagaGrpcMarshallers.CreateRequestMarshaller(Ser<SagaControlRequest>());

        var decoded = RoundTrip(marshaller, new SagaControlRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SagaResponseMarshaller_single_segment_round_trip_succeeds()
    {
        var marshaller = LatticeSagaGrpcMarshallers.CreateResponseMarshaller(Ser<SagaControlResponse>());

        var decoded = RoundTrip(marshaller, new SagaControlResponseBox { Value = default }, multiSegment: false);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SagaResponseMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeSagaGrpcMarshallers.CreateResponseMarshaller(Ser<SagaControlResponse>());

        var decoded = RoundTrip(marshaller, new SagaControlResponseBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotRequestMarshaller_single_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateRequestMarshaller(Ser<RemoteSnapshotMetadataRequest>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotMetadataRequestBox { Value = default }, multiSegment: false);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateRequestMarshaller(Ser<RemoteSnapshotMetadataRequest>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotMetadataRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotMetadataMarshaller_single_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateMetadataMarshaller(Ser<RemoteSnapshotMetadata>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotMetadataBox { Value = default }, multiSegment: false);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotMetadataMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateMetadataMarshaller(Ser<RemoteSnapshotMetadata>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotMetadataBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotStreamItemMarshaller_single_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateStreamItemMarshaller(Ser<RemoteSnapshotStreamItem>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotStreamItemBox { Value = default }, multiSegment: false);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void SnapshotStreamItemMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateStreamItemMarshaller(Ser<RemoteSnapshotStreamItem>());

        var decoded = RoundTrip(marshaller, new RemoteSnapshotStreamItemBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }
}
