using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
public class LatticeReplicationGrpcMarshallersTests
{
    private ServiceProvider _sp = null!;
    private Serializer<ReplicationBatchEnvelope> _envSerializer = null!;
    private Serializer<ReplicationAck> _ackSerializer = null!;
    private Serializer<DigestProbeRequest> _probeRequestSerializer = null!;
    private Serializer<DigestProbeResponse> _probeResponseSerializer = null!;

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _envSerializer = _sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _ackSerializer = _sp.GetRequiredService<Serializer<ReplicationAck>>();
        _probeRequestSerializer = _sp.GetRequiredService<Serializer<DigestProbeRequest>>();
        _probeResponseSerializer = _sp.GetRequiredService<Serializer<DigestProbeResponse>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _sp.Dispose();

    [Test]
    public void CreateEnvelopeMarshaller_throws_when_encoder_null()
    {
        var walEncoder = new OrleansBinaryWalRecordEncoder(_sp.GetRequiredService<Serializer<WalRecord>>());
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(null!, walEncoder),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateEnvelopeMarshaller_throws_when_walRecordEncoder_null()
    {
        var encoder = new TestEncoder(_envSerializer);
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(encoder, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateAckMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateAckMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Envelope_round_trip_via_orleans_serializer_preserves_fields()
    {
        // We don't drive the gRPC SerializationContext directly (no public ctor);
        // instead, validate that the underlying encoder + Orleans serializer
        // round-trip. The marshaller wires both into gRPC's contextual API.
        var encoder = new TestEncoder(_envSerializer);

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
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

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(envelope, writer);
        var decoded = encoder.Decode(writer.WrittenMemory);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeName, Is.EqualTo("tree"));
            Assert.That(decoded.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(decoded.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.Entries[0].Key, Is.EqualTo("k"));
            Assert.That(decoded.Entries[0].Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void Ack_round_trip_via_orleans_serializer_preserves_fields()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 7 };
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };

        var writer = new ArrayBufferWriter<byte>();
        _ackSerializer.Serialize(ack, writer);
        var decoded = _ackSerializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Accepted, Is.True);
            Assert.That(decoded.HighestAppliedHlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void EnvelopeBox_carries_value_verbatim()
    {
        var envelope = new ReplicationBatchEnvelope { TreeName = "t", OriginClusterId = "o" };
        var box = new ReplicationBatchEnvelopeBox { Value = envelope };
        Assert.That(box.Value, Is.EqualTo(envelope));
    }

    [Test]
    public void AckBox_carries_value_verbatim()
    {
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 } };
        var box = new ReplicationAckBox { Value = ack };
        Assert.That(box.Value, Is.EqualTo(ack));
    }

    [Test]
    public void CreateProbeRequestMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateProbeRequestMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateProbeResponseMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateProbeResponseMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeRequest_round_trip_via_orleans_serializer_preserves_fields()
    {
        var request = new DigestProbeRequest { TreeName = "orders", ShardIndex = 7 };

        var writer = new ArrayBufferWriter<byte>();
        _probeRequestSerializer.Serialize(request, writer);
        var decoded = _probeRequestSerializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeName, Is.EqualTo("orders"));
            Assert.That(decoded.ShardIndex, Is.EqualTo(7));
        });
    }

    [Test]
    public void ProbeResponse_round_trip_via_orleans_serializer_preserves_fields()
    {
        var response = new DigestProbeResponse
        {
            DigestAvailable = true,
            Digest = new LeafProjectionDigest
            {
                Hash = new byte[] { 9, 8, 7 },
                EntryCount = 11,
                CheckpointOffset = 42,
                Version = LeafProjectionDigest.CurrentVersion,
            },
        };

        var writer = new ArrayBufferWriter<byte>();
        _probeResponseSerializer.Serialize(response, writer);
        var decoded = _probeResponseSerializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.DigestAvailable, Is.True);
            Assert.That(decoded.Digest.Hash, Is.EqualTo(new byte[] { 9, 8, 7 }));
            Assert.That(decoded.Digest.EntryCount, Is.EqualTo(11));
            Assert.That(decoded.Digest.CheckpointOffset, Is.EqualTo(42));
            Assert.That(decoded.Digest.Version, Is.EqualTo(LeafProjectionDigest.CurrentVersion));
        });
    }

    [Test]
    public void ProbeRequestBox_carries_value_verbatim()
    {
        var request = new DigestProbeRequest { TreeName = "t", ShardIndex = 3 };
        var box = new DigestProbeRequestBox { Value = request };
        Assert.That(box.Value, Is.EqualTo(request));
    }

    [Test]
    public void ProbeResponseBox_carries_value_verbatim()
    {
        var response = new DigestProbeResponse { DigestAvailable = false };
        var box = new DigestProbeResponseBox { Value = response };
        Assert.That(box.Value, Is.EqualTo(response));
    }

    [Test]
    public void CreateContentManifestRequestMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateContentManifestRequestMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateContentManifestResponseMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateContentManifestResponseMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ContentManifestRequest_round_trip_via_orleans_serializer_preserves_fields()
    {
        var serializer = _sp.GetRequiredService<Serializer<ContentManifestRequest>>();
        var request = new ContentManifestRequest
        {
            TreeName = "orders",
            OriginClusterId = "site-a",
            Entries = new[]
            {
                new ContentManifestEntry
                {
                    EntryIndex = 3,
                    Key = "k",
                    ContentHash = 0xDEADBEEFUL,
                    Hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 1 },
                },
            },
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(request, writer);
        var decoded = serializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.TreeName, Is.EqualTo("orders"));
            Assert.That(decoded.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(decoded.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.Entries[0].EntryIndex, Is.EqualTo(3));
            Assert.That(decoded.Entries[0].Key, Is.EqualTo("k"));
            Assert.That(decoded.Entries[0].ContentHash, Is.EqualTo(0xDEADBEEFUL));
        });
    }

    [Test]
    public void ContentManifestResponse_round_trip_via_orleans_serializer_preserves_fields()
    {
        var serializer = _sp.GetRequiredService<Serializer<ContentManifestResponse>>();
        var response = new ContentManifestResponse
        {
            ExchangeSupported = true,
            MissingEntryIndices = new[] { 1, 4, 9 },
            AdvancedHlc = new HybridLogicalClock { WallClockTicks = 12, Counter = 3 },
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(response, writer);
        var decoded = serializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ExchangeSupported, Is.True);
            Assert.That(decoded.MissingEntryIndices, Is.EqualTo(new[] { 1, 4, 9 }));
            Assert.That(decoded.AdvancedHlc, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 12, Counter = 3 }));
        });
    }

    [Test]
    public void ContentManifestRequestBox_carries_value_verbatim()
    {
        var request = new ContentManifestRequest { TreeName = "t", OriginClusterId = "o" };
        var box = new ContentManifestRequestBox { Value = request };
        Assert.That(box.Value, Is.EqualTo(request));
    }

    [Test]
    public void ContentManifestResponseBox_carries_value_verbatim()
    {
        var response = ContentManifestResponse.NotSupported;
        var box = new ContentManifestResponseBox { Value = response };
        Assert.That(box.Value, Is.EqualTo(response));
    }

    [Test]
    public void CreateCompressionDictionaryPullRequestMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullRequestMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateCompressionDictionaryPullResponseMarshaller_throws_when_serializer_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateCompressionDictionaryPullResponseMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CompressionDictionaryPullRequest_round_trip_via_orleans_serializer_preserves_fields()
    {
        var serializer = _sp.GetRequiredService<Serializer<CompressionDictionaryPullRequest>>();
        var request = new CompressionDictionaryPullRequest { DictionaryId = 42u };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(request, writer);
        var decoded = serializer.Deserialize(writer.WrittenMemory.Span);

        Assert.That(decoded.DictionaryId, Is.EqualTo(42u));
    }

    [Test]
    public void CompressionDictionaryPullResponse_round_trip_via_orleans_serializer_preserves_fields()
    {
        var serializer = _sp.GetRequiredService<Serializer<CompressionDictionaryPullResponse>>();
        var response = new CompressionDictionaryPullResponse
        {
            ExchangeSupported = true,
            Found = true,
            DictionaryId = 11u,
            Fingerprint = 0xABCDUL,
            Dictionary = new byte[] { 5, 6, 7 },
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(response, writer);
        var decoded = serializer.Deserialize(writer.WrittenMemory.Span);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ExchangeSupported, Is.True);
            Assert.That(decoded.Found, Is.True);
            Assert.That(decoded.DictionaryId, Is.EqualTo(11u));
            Assert.That(decoded.Fingerprint, Is.EqualTo(0xABCDUL));
            Assert.That(decoded.Dictionary.ToArray(), Is.EqualTo(new byte[] { 5, 6, 7 }));
        });
    }

    [Test]
    public void CompressionDictionaryPullRequestBox_carries_value_verbatim()
    {
        var request = new CompressionDictionaryPullRequest { DictionaryId = 3u };
        var box = new CompressionDictionaryPullRequestBox { Value = request };
        Assert.That(box.Value, Is.EqualTo(request));
    }

    [Test]
    public void CompressionDictionaryPullResponseBox_carries_value_verbatim()
    {
        var response = CompressionDictionaryPullResponse.NotHeld;
        var box = new CompressionDictionaryPullResponseBox { Value = response };
        Assert.That(box.Value, Is.EqualTo(response));
    }
}

