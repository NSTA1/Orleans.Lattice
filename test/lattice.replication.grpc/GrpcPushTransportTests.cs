using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
public class GrpcPushTransportTests
{
    private sealed class StubEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "test/stub";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) { }
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => default;
    }

    private static IOptionsMonitor<GrpcPushTransportOptions> OptionsFor(GrpcPushTransportOptions opts)
    {
        var monitor = Substitute.For<IOptionsMonitor<GrpcPushTransportOptions>>();
        monitor.CurrentValue.Returns(opts);
        return monitor;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> ReplicationOptionsFor(string clusterId = "self")
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    private static IReplicationSecretProvider SecretsStub()
    {
        var s = Substitute.For<IReplicationSecretProvider>();
        s.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("test-secret"));
        s.GetAcceptedSecretsAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeReplicationAcceptedSecrets>(
                new LatticeReplicationAcceptedSecrets(new[] { "test-secret" }, "v1")));
        s.IsAcceptedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<bool>(true));
        return s;
    }

    private static GrpcPushTransport CreateTransport(GrpcPushTransportOptions? opts = null)
    {
        var encoder = new StubEncoder();
        var ackSerializer = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Orleans.Serialization.Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);
        return new GrpcPushTransport(
            method,
            encoder,
            OptionsFor(opts ?? new GrpcPushTransportOptions { AllowPlaintextEndpoints = true }),
            SecretsStub(),
            ReplicationOptionsFor());
    }

    private static ReplicationBatch MakeBatch(
        string target = "peer",
        string tree = "tree",
        string origin = "self",
        byte[]? payload = null)
        => new()
        {
            TargetClusterId = target,
            TreeName = tree,
            OriginClusterId = origin,
            Payload = payload ?? Array.Empty<byte>(),
        };

    [Test]
    public void Constructor_throws_when_method_null()
    {
        Assert.That(
            () => new GrpcPushTransport(null!, new StubEncoder(), OptionsFor(new GrpcPushTransportOptions()), SecretsStub(), ReplicationOptionsFor()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_encoder_null()
    {
        var encoder = new StubEncoder();
        var ackSerializer = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Orleans.Serialization.Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);

        Assert.That(
            () => new GrpcPushTransport(method, null!, OptionsFor(new GrpcPushTransportOptions()), SecretsStub(), ReplicationOptionsFor()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_options_null()
    {
        var encoder = new StubEncoder();
        var ackSerializer = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Orleans.Serialization.Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);

        Assert.That(
            () => new GrpcPushTransport(method, encoder, null!, SecretsStub(), ReplicationOptionsFor()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_secrets_null()
    {
        var encoder = new StubEncoder();
        var ackSerializer = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Orleans.Serialization.Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);

        Assert.That(
            () => new GrpcPushTransport(method, encoder, OptionsFor(new GrpcPushTransportOptions()), null!, ReplicationOptionsFor()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_replication_options_null()
    {
        var encoder = new StubEncoder();
        var ackSerializer = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Orleans.Serialization.Serializer<ReplicationAck>>();
        var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);

        Assert.That(
            () => new GrpcPushTransport(method, encoder, OptionsFor(new GrpcPushTransportOptions()), SecretsStub(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_empty()
    {
        using var transport = CreateTransport();
        Assert.That(
            async () => await transport.SendAsync(MakeBatch(target: string.Empty), CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_tree_name_empty()
    {
        using var transport = CreateTransport();
        Assert.That(
            async () => await transport.SendAsync(MakeBatch(tree: string.Empty), CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_origin_cluster_id_empty()
    {
        using var transport = CreateTransport();
        Assert.That(
            async () => await transport.SendAsync(MakeBatch(origin: string.Empty), CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_peer_endpoint_not_configured()
    {
        using var transport = CreateTransport(); // empty PeerEndpoints
        Assert.That(
            async () => await transport.SendAsync(MakeBatch(target: "unknown-peer"), CancellationToken.None),
            Throws.InvalidOperationException);
    }

    [Test]
    public void SendAsync_throws_object_disposed_after_dispose()
    {
        var transport = CreateTransport();
        transport.Dispose();
        Assert.That(
            async () => await transport.SendAsync(MakeBatch(), CancellationToken.None),
            Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var transport = CreateTransport();
        transport.Dispose();
        Assert.DoesNotThrow(transport.Dispose);
    }
}





