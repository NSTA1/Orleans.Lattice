using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeRemoteSnapshotGrpcMarshallers"/>.
/// Confirms the request, metadata, and stream-item marshallers round
/// trip the Orleans-serialised payload byte-for-byte across the gRPC
/// serialiser/deserialiser pair.
/// </summary>
[TestFixture]
public class LatticeRemoteSnapshotGrpcMarshallersTests
{
    private static (Serializer<RemoteSnapshotMetadataRequest> req,
                    Serializer<RemoteSnapshotMetadata> meta,
                    Serializer<RemoteSnapshotStreamItem> item)
        BuildSerializers()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return (
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>());
    }

    private static byte[] SerializeRequest(global::Grpc.Core.Marshaller<RemoteSnapshotMetadataRequestBox> marshaller, RemoteSnapshotMetadataRequest value)
    {
        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(new RemoteSnapshotMetadataRequestBox { Value = value }, context);
        return context.Buffer.ToArray();
    }

    [Test]
    public void Request_marshaller_round_trips_payload()
    {
        var (reqS, _, _) = BuildSerializers();
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateRequestMarshaller(reqS);

        var original = new RemoteSnapshotMetadataRequest
        {
            TreeName = "tree",
            SourceClusterId = "site-a",
            FromAsOfHlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 },
        };

        var bytes = SerializeRequest(marshaller, original);
        var ctx = new FakeDeserializationContext(bytes);
        var roundTripped = marshaller.ContextualDeserializer(ctx);

        Assert.That(roundTripped.Value, Is.EqualTo(original));
    }

    [Test]
    public void Metadata_marshaller_round_trips_payload()
    {
        var (_, metaS, _) = BuildSerializers();
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateMetadataMarshaller(metaS);

        var original = new RemoteSnapshotMetadata
        {
            TreeName = "tree",
            SourceClusterId = "site-a",
            AsOfHlc = new HybridLogicalClock { WallClockTicks = 200, Counter = 3 },
            CausalStableFrontier = new VersionVector(),
        };

        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(new RemoteSnapshotMetadataBox { Value = original }, context);
        var ctx = new FakeDeserializationContext(context.Buffer.ToArray());
        var roundTripped = marshaller.ContextualDeserializer(ctx);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.Value.TreeName, Is.EqualTo(original.TreeName));
            Assert.That(roundTripped.Value.SourceClusterId, Is.EqualTo(original.SourceClusterId));
            Assert.That(roundTripped.Value.AsOfHlc, Is.EqualTo(original.AsOfHlc));
        });
    }

    [Test]
    public void StreamItem_marshaller_round_trips_payload()
    {
        var (_, _, itemS) = BuildSerializers();
        var marshaller = LatticeRemoteSnapshotGrpcMarshallers.CreateStreamItemMarshaller(itemS);

        var original = new RemoteSnapshotStreamItem
        {
            Entry = new SnapshotEntry
            {
                Key = "k",
                Value = new byte[] { 1, 2, 3 },
                Timestamp = new HybridLogicalClock { WallClockTicks = 42, Counter = 0 },
            },
        };

        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(new RemoteSnapshotStreamItemBox { Value = original }, context);
        var ctx = new FakeDeserializationContext(context.Buffer.ToArray());
        var roundTripped = marshaller.ContextualDeserializer(ctx);

        Assert.That(roundTripped.Value.Entry.Key, Is.EqualTo(original.Entry.Key));
        Assert.That(roundTripped.Value.Entry.Value, Is.EqualTo(original.Entry.Value));
        Assert.That(roundTripped.Value.Entry.Timestamp, Is.EqualTo(original.Entry.Timestamp));
    }

    [Test]
    public void CreateRequestMarshaller_throws_on_null_serializer()
    {
        Assert.That(() => LatticeRemoteSnapshotGrpcMarshallers.CreateRequestMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateMetadataMarshaller_throws_on_null_serializer()
    {
        Assert.That(() => LatticeRemoteSnapshotGrpcMarshallers.CreateMetadataMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateStreamItemMarshaller_throws_on_null_serializer()
    {
        Assert.That(() => LatticeRemoteSnapshotGrpcMarshallers.CreateStreamItemMarshaller(null!),
            Throws.ArgumentNullException);
    }

    private sealed class FakeSerializationContext : global::Grpc.Core.SerializationContext
    {
        public System.IO.MemoryStream Buffer { get; } = new();
        private System.Buffers.ArrayBufferWriter<byte>? _writer;

        public override void Complete(byte[] payload) => Buffer.Write(payload, 0, payload.Length);

        public override void Complete()
        {
            if (_writer is not null)
            {
                Buffer.Write(_writer.WrittenSpan);
            }
        }

        public override System.Buffers.IBufferWriter<byte> GetBufferWriter()
        {
            _writer ??= new System.Buffers.ArrayBufferWriter<byte>();
            return _writer;
        }
    }

    private sealed class FakeDeserializationContext : global::Grpc.Core.DeserializationContext
    {
        private readonly byte[] _payload;

        public FakeDeserializationContext(byte[] payload) => _payload = payload;

        public override int PayloadLength => _payload.Length;

        public override byte[] PayloadAsNewBuffer() => (byte[])_payload.Clone();

        public override System.Buffers.ReadOnlySequence<byte> PayloadAsReadOnlySequence()
            => new System.Buffers.ReadOnlySequence<byte>(_payload);
    }
}