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
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _sp.Dispose();

    [Test]
    public void CreateEnvelopeMarshaller_throws_when_encoder_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcMarshallers.CreateEnvelopeMarshaller(null!),
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
}

