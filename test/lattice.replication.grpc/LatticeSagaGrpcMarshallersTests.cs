using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSagaGrpcMarshallers"/>. Confirms the
/// request and response marshallers round trip the Orleans-serialised
/// payload byte-for-byte across the gRPC serialiser/deserialiser pair.
/// </summary>
[TestFixture]
public class LatticeSagaGrpcMarshallersTests
{
    private static (Serializer<SagaControlRequest> req, Serializer<SagaControlResponse> resp) BuildSerializers()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return (
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
    }

    [Test]
    public void Request_marshaller_round_trips_payload()
    {
        var (reqS, _) = BuildSerializers();
        var marshaller = LatticeSagaGrpcMarshallers.CreateRequestMarshaller(reqS);

        var original = new SagaControlRequest
        {
            SagaId = "saga-1",
            TargetTree = "tree",
            ManifestId = "manifest-7",
            CoordinatorClusterId = "site-a",
        };

        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(new SagaControlRequestBox { Value = original }, context);
        var ctx = new FakeDeserializationContext(context.Buffer.ToArray());
        var roundTripped = marshaller.ContextualDeserializer(ctx);

        Assert.That(roundTripped.Value, Is.EqualTo(original));
    }

    [Test]
    public void Response_marshaller_round_trips_payload()
    {
        var (_, respS) = BuildSerializers();
        var marshaller = LatticeSagaGrpcMarshallers.CreateResponseMarshaller(respS);

        var original = new SagaControlResponse
        {
            SagaId = "saga-1",
            Phase = SagaPhase.Prepared,
            Vote = SagaVote.Commit,
            Detail = "prepared",
        };

        var context = new FakeSerializationContext();
        marshaller.ContextualSerializer(new SagaControlResponseBox { Value = original }, context);
        var ctx = new FakeDeserializationContext(context.Buffer.ToArray());
        var roundTripped = marshaller.ContextualDeserializer(ctx);

        Assert.That(roundTripped.Value, Is.EqualTo(original));
    }

    [Test]
    public void CreateRequestMarshaller_throws_on_null_serializer()
    {
        Assert.That(() => LatticeSagaGrpcMarshallers.CreateRequestMarshaller(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CreateResponseMarshaller_throws_on_null_serializer()
    {
        Assert.That(() => LatticeSagaGrpcMarshallers.CreateResponseMarshaller(null!),
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
