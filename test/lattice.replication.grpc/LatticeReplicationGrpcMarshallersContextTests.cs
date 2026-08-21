using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;
using static Orleans.Lattice.Replication.Grpc.Tests.FakeGrpcSerializationContexts;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Drives the gRPC <see cref="global::Grpc.Core.Marshaller{T}"/> contextual
/// serializer/deserializer delegates produced by the replication-grpc
/// marshaller factories directly, using in-process fake gRPC contexts.
/// This exercises the buffer-writer serialize hand-off, the
/// single-segment deserialize fast path, and the multi-segment pooled
/// fallback for every marshaller - the paths a plain
/// serializer-round-trip test cannot reach because they live inside the
/// gRPC contextual delegates.
/// </summary>
[TestFixture]
public class LatticeReplicationGrpcMarshallersContextTests
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

    private IReplicationBatchEncoder CreateEnvelopeEncoder()
        => new OrleansBinaryReplicationBatchEncoder(Ser<ReplicationBatchEnvelope>());

    [Test]
    public void AckMarshaller_single_segment_round_trip_preserves_value()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateAckMarshaller(Ser<ReplicationAck>());
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 1 } };

        var decoded = RoundTrip(marshaller, new ReplicationAckBox { Value = ack }, multiSegment: false);

        Assert.That(decoded.Value.Accepted, Is.True);
    }

    [Test]
    public void AckMarshaller_multi_segment_round_trip_preserves_value()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateAckMarshaller(Ser<ReplicationAck>());
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 9, Counter = 3 } };

        var decoded = RoundTrip(marshaller, new ReplicationAckBox { Value = ack }, multiSegment: true);

        Assert.That(decoded.Value.HighestAppliedHlc.WallClockTicks, Is.EqualTo(9));
    }

    [Test]
    public void ProbeRequestMarshaller_multi_segment_round_trip_preserves_value()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateProbeRequestMarshaller(Ser<DigestProbeRequest>());
        var request = new DigestProbeRequest { TreeName = "orders", ShardIndex = 3 };

        var decoded = RoundTrip(marshaller, new DigestProbeRequestBox { Value = request }, multiSegment: true);

        Assert.That(decoded.Value.TreeName, Is.EqualTo("orders"));
    }

    [Test]
    public void ProbeResponseMarshaller_single_segment_round_trip_preserves_value()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateProbeResponseMarshaller(Ser<DigestProbeResponse>());
        var response = new DigestProbeResponse { DigestAvailable = false };

        var decoded = RoundTrip(marshaller, new DigestProbeResponseBox { Value = response }, multiSegment: false);

        Assert.That(decoded.Value.DigestAvailable, Is.False);
    }

    [Test]
    public void ContentManifestRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateContentManifestRequestMarshaller(Ser<ContentManifestRequest>());

        var decoded = RoundTrip(marshaller, new ContentManifestRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void ContentManifestResponseMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateContentManifestResponseMarshaller(Ser<ContentManifestResponse>());

        var decoded = RoundTrip(marshaller, new ContentManifestResponseBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void CompressionDictionaryPullRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullRequestMarshaller(Ser<CompressionDictionaryPullRequest>());

        var decoded = RoundTrip(marshaller, new CompressionDictionaryPullRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void CompressionDictionaryPullResponseMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullResponseMarshaller(Ser<CompressionDictionaryPullResponse>());

        var decoded = RoundTrip(marshaller, new CompressionDictionaryPullResponseBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void MerkleWalkProbeRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateMerkleWalkProbeRequestMarshaller(Ser<MerkleWalkProbeRequest>());

        var decoded = RoundTrip(marshaller, new MerkleWalkProbeRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void MerkleWalkProbeResponseMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreateMerkleWalkProbeResponseMarshaller(Ser<MerkleWalkProbeResponse>());

        var decoded = RoundTrip(marshaller, new MerkleWalkProbeResponseBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void PeerHighWaterMarkRequestMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreatePeerHighWaterMarkRequestMarshaller(Ser<PeerHighWaterMarkRequest>());

        var decoded = RoundTrip(marshaller, new PeerHighWaterMarkRequestBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void PeerHighWaterMarkResponseMarshaller_multi_segment_round_trip_succeeds()
    {
        var marshaller = LatticeReplicationGrpcMarshallers.CreatePeerHighWaterMarkResponseMarshaller(Ser<PeerHighWaterMarkResponse>());

        var decoded = RoundTrip(marshaller, new PeerHighWaterMarkResponseBox { Value = default }, multiSegment: true);

        Assert.That(decoded, Is.Not.Null);
    }

    [Test]
    public void EnvelopeMarshaller_typed_path_multi_segment_round_trip_preserves_entries()
    {
        var encoder = CreateEnvelopeEncoder();
        var walEncoder = GrpcTestFactories.CreateWalRecordEncoder();
        var marshaller = LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder, walEncoder);

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = encoder.CurrentWireVersion,
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[]
            {
                new WalRecord
                {
                    TreeId = "tree",
                    Op = MutationKind.Set,
                    Key = "k",
                    Value = new byte[] { 1, 2, 3 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 42, Counter = 7 },
                    OriginClusterId = "site-a",
                    Mode = LatticeMergeMode.LwwRegister,
                },
            },
        };

        var decoded = RoundTrip(marshaller, new ReplicationBatchEnvelopeBox { Value = envelope }, multiSegment: true);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Value.TreeName, Is.EqualTo("tree"));
            Assert.That(decoded.Value.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.Value.Entries[0].Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public void EnvelopeMarshaller_framing_path_round_trip_inflates_entries()
    {
        var encoder = CreateEnvelopeEncoder();
        var walEncoder = GrpcTestFactories.CreateWalRecordEncoder();
        var marshaller = LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder, walEncoder);

        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "framed-key",
            Value = new byte[] { 9, 9, 9 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 2 },
            OriginClusterId = "site-b",
            Mode = LatticeMergeMode.LwwRegister,
        };

        var entryWriter = new ArrayBufferWriter<byte>();
        walEncoder.Encode(record, entryWriter);
        var entrySegment = new ArraySegment<byte>(entryWriter.WrittenMemory.ToArray());

        var header = new EncodedBatchHeader
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-b"),
            EntryCount = 1,
            BatchSequence = 1,
            AtomicBatchSpanCount = 0,
            Mode = LatticeMergeMode.LwwRegister,
            Compression = LatticeCompression.None,
        };

        var framing = new ReplicationBatchEnvelopeBox.FramingPayload(
            header,
            "tree",
            "site-b",
            new ReadOnlyMemory<ArraySegment<byte>>(new[] { entrySegment }));

        var box = new ReplicationBatchEnvelopeBox { Framing = framing };

        var decoded = RoundTrip(marshaller, box, multiSegment: false);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Value.TreeName, Is.EqualTo("tree"));
            Assert.That(decoded.Value.OriginClusterId, Is.EqualTo("site-b"));
            Assert.That(decoded.Value.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.Value.Entries[0].Key, Is.EqualTo("framed-key"));
            Assert.That(decoded.Value.Entries[0].TreeId, Is.EqualTo("tree"));
        });
    }

    [Test]
    public void EnvelopeMarshaller_framing_path_multi_segment_round_trip_inflates_entries()
    {
        var encoder = CreateEnvelopeEncoder();
        var walEncoder = GrpcTestFactories.CreateWalRecordEncoder();
        var marshaller = LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder, walEncoder);

        var record = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = "framed-multi",
            Value = new byte[] { 4, 5, 6, 7 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 200, Counter = 4 },
            OriginClusterId = "site-c",
            Mode = LatticeMergeMode.LwwRegister,
        };

        var entryWriter = new ArrayBufferWriter<byte>();
        walEncoder.Encode(record, entryWriter);
        var entrySegment = new ArraySegment<byte>(entryWriter.WrittenMemory.ToArray());

        var header = new EncodedBatchHeader
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-c"),
            EntryCount = 1,
            BatchSequence = 2,
            AtomicBatchSpanCount = 0,
            Mode = LatticeMergeMode.LwwRegister,
            Compression = LatticeCompression.None,
        };

        var framing = new ReplicationBatchEnvelopeBox.FramingPayload(
            header,
            "tree",
            "site-c",
            new ReadOnlyMemory<ArraySegment<byte>>(new[] { entrySegment }));

        var decoded = RoundTrip(marshaller, new ReplicationBatchEnvelopeBox { Framing = framing }, multiSegment: true);

        Assert.That(decoded.Value.Entries[0].Key, Is.EqualTo("framed-multi"));
    }
}
