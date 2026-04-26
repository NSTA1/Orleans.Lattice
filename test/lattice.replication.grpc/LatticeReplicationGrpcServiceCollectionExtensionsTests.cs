using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
public class LatticeReplicationGrpcServiceCollectionExtensionsTests
{
    private sealed class StubEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "test/stub";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer) { }
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => default;
    }

    private sealed class StubTransport : IReplicationTransport
    {
        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
            => Task.FromResult(default(ReplicationAck));
    }

    private static IServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLogging();
        services.TryAddSingleton<IReplicationBatchEncoder, StubEncoder>();
        services.TryAddSingleton<IReplicationApplier>(_ => Substitute.For<IReplicationApplier>());
        return services;
    }

    [Test]
    public void AddLatticeReplicationGrpcPushTransport_throws_when_services_is_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcPushTransport(null!, _ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationGrpcPushTransport_throws_when_configure_is_null()
    {
        var services = new ServiceCollection();
        Assert.That(
            () => services.AddLatticeReplicationGrpcPushTransport(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationGrpcPushTransport_replaces_default_transport()
    {
        var services = BaseServices();
        services.TryAddSingleton<IReplicationTransport, StubTransport>();

        services.AddLatticeReplicationGrpcPushTransport(opts =>
        {
            opts.PeerEndpoints["site-b"] = new Uri("https://localhost:5001/");
        });

        using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredService<IReplicationTransport>();

        Assert.That(transport, Is.InstanceOf<GrpcPushTransport>());
    }

    [Test]
    public void AddLatticeReplicationGrpcPushTransport_binds_options()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpcPushTransport(opts =>
        {
            opts.PeerEndpoints["site-b"] = new Uri("https://localhost:5001/");
        });

        using var sp = services.BuildServiceProvider();
        var monitor = sp.GetRequiredService<IOptionsMonitor<GrpcPushTransportOptions>>();

        Assert.That(monitor.CurrentValue.PeerEndpoints["site-b"], Is.EqualTo(new Uri("https://localhost:5001/")));
    }

    [Test]
    public void AddLatticeReplicationGrpcServer_throws_when_services_is_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcServer(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationGrpcServer_registers_method_singleton()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpcServer();

        using var sp = services.BuildServiceProvider();
        var method1 = sp.GetRequiredService<LatticeReplicationGrpcMethod>();
        var method2 = sp.GetRequiredService<LatticeReplicationGrpcMethod>();
        var svc = sp.GetRequiredService<LatticeReplicationGrpcService>();

        Assert.Multiple(() =>
        {
            Assert.That(method1, Is.SameAs(method2));
            Assert.That(svc, Is.Not.Null);
        });
    }

    [Test]
    public void MapLatticeReplicationGrpcService_throws_when_endpoints_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpcService(null!),
            Throws.ArgumentNullException);
    }
}


