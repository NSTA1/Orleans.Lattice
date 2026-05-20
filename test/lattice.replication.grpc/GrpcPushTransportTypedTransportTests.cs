using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Surface tests pinning the gRPC streaming push transport's
/// implementation of <see cref="ITypedReplicationTransport"/>. The
/// outbound shipper's typed-transport fast path relies on the
/// `as ITypedReplicationTransport` probe matching this concrete type.
/// </summary>
[TestFixture]
public sealed class GrpcPushTransportTypedTransportTests
{
    private sealed class StubDecoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer) { }
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => default;
    }

    private static IOptionsMonitor<T> OptionsFor<T>(T value) where T : class
    {
        var monitor = Substitute.For<IOptionsMonitor<T>>();
        monitor.CurrentValue.Returns(value);
        monitor.Get(Arg.Any<string>()).Returns(value);
        return monitor;
    }

    private static IReplicationSecretProvider SecretsStub()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("secret"));
        s.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(
                new LatticeReplicationAcceptedSecrets(new[] { "secret" }, "v1")));
        s.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        return s;
    }

    private static GrpcPushTransport CreateTransport()
    {
        var encoder = new StubDecoder();
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, new OrleansBinaryWalRecordEncoder(sp.GetRequiredService<Serializer<WalRecord>>()), ackSerializer);
        return new GrpcPushTransport(
            method,
            encoder,
            OptionsFor(new GrpcPushTransportOptions { AllowPlaintextEndpoints = true }),
            SecretsStub(),
            OptionsFor(new LatticeReplicationOptions { ClusterId = "local" }));
    }

    [Test]
    public void GrpcPushTransport_implements_ITypedReplicationTransport()
    {
        // The shipper's runtime probe (`transport as ITypedReplicationTransport`)
        // depends on the concrete type implementing the typed interface;
        // pin it so a refactor of the transport's declared interfaces
        // cannot silently regress the dead-encode elimination.
        Assert.That(typeof(ITypedReplicationTransport).IsAssignableFrom(typeof(GrpcPushTransport)),
            Is.True,
            "GrpcPushTransport must implement ITypedReplicationTransport so the outbound shipper's typed-transport fast path applies to it");
    }

    [Test]
    public void GrpcPushTransport_instance_matches_typed_probe()
    {
        // Mirror of the type-level test, but at instance level - this
        // is the exact pattern the shipper uses to decide whether to
        // skip the encode.
        using var transport = CreateTransport();
        var typed = transport as ITypedReplicationTransport;
        Assert.That(typed, Is.Not.Null,
            "an instance of GrpcPushTransport must match the `as ITypedReplicationTransport` probe");
    }
}
