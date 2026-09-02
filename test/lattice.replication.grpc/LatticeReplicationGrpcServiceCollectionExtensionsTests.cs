using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice;
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
        services.TryAddSingleton<ISnapshotProvider>(_ => Substitute.For<ISnapshotProvider>());
        // The gRPC receiver service resolves the local ILattice grain to
        // answer inbound digest probes, so it now takes an IGrainFactory.
        // Substitute it here (no silo in these unit tests).
        services.TryAddSingleton<IGrainFactory>(_ => Substitute.For<IGrainFactory>());
        return services;
    }

    [Test]
    public void AddLatticeReplicationGrpc_throws_when_services_is_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpc(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationGrpc_with_no_configure_registers_default_options()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc();

        using var sp = services.BuildServiceProvider();
        var monitor = sp.GetRequiredService<IOptionsMonitor<LatticeReplicationGrpcOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.CurrentValue.Peers, Is.Empty);
            Assert.That(monitor.CurrentValue.AllowPlaintextEndpoints, Is.False);
            Assert.That(monitor.CurrentValue.LocalClusterId, Is.Null);
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_replaces_push_transport()
    {
        var services = BaseServices();
        services.TryAddSingleton<IReplicationTransport, StubTransport>();

        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-b"] = new Uri("https://localhost:5001/");
        });

        using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredService<IReplicationTransport>();

        Assert.That(transport, Is.InstanceOf<GrpcPushTransport>());
    }

    [Test]
    public void AddLatticeReplicationGrpc_registers_snapshot_transport()
    {
        var services = BaseServices();

        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-a"] = new Uri("https://localhost:5002/");
        });

        using var sp = services.BuildServiceProvider();
        var transport = sp.GetRequiredService<IRemoteSnapshotTransport>();
        Assert.That(transport, Is.InstanceOf<GrpcRemoteSnapshotTransport>());
    }

    [Test]
    public void AddLatticeReplicationGrpc_projects_unified_options_to_push_options()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-b"] = new Uri("https://localhost:5001/");
            opts.AllowPlaintextEndpoints = true;
            opts.LocalClusterId = "site-a";
        });

        using var sp = services.BuildServiceProvider();
        var pushOptions = sp.GetRequiredService<IOptionsMonitor<GrpcPushTransportOptions>>().CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(pushOptions.PeerEndpoints["site-b"], Is.EqualTo(new Uri("https://localhost:5001/")));
            Assert.That(pushOptions.AllowPlaintextEndpoints, Is.True);
            Assert.That(pushOptions.LocalClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_projects_unified_options_to_snapshot_options()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-a"] = new Uri("https://localhost:5002/");
            opts.AllowPlaintextEndpoints = true;
            opts.LocalClusterId = "site-b";
        });

        using var sp = services.BuildServiceProvider();
        var snapOptions = sp.GetRequiredService<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>().CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(snapOptions.SenderEndpoints["site-a"], Is.EqualTo(new Uri("https://localhost:5002/")));
            Assert.That(snapOptions.AllowPlaintextEndpoints, Is.True);
            Assert.That(snapOptions.LocalClusterId, Is.EqualTo("site-b"));
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_projects_unified_options_to_saga_control_options()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-c"] = new Uri("https://localhost:5003/");
            opts.AllowPlaintextEndpoints = true;
            opts.LocalClusterId = "site-a";
        });

        using var sp = services.BuildServiceProvider();
        var sagaOptions = sp.GetRequiredService<IOptionsMonitor<GrpcSagaControlChannelOptions>>().CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(sagaOptions.PeerEndpoints["site-c"], Is.EqualTo(new Uri("https://localhost:5003/")));
            Assert.That(sagaOptions.AllowPlaintextEndpoints, Is.True);
            Assert.That(sagaOptions.LocalClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_projects_the_configure_channel_hook_onto_every_transport()
    {
        var services = BaseServices();
        Action<string, global::Grpc.Net.Client.GrpcChannelOptions> hook = (_, _) => { };

        services.AddLatticeReplicationGrpc(opts =>
        {
            opts.Peers["site-d"] = new Uri("https://localhost:5004/");
            opts.ConfigureChannel = hook;
        });

        using var sp = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(sp.GetRequiredService<IOptionsMonitor<GrpcPushTransportOptions>>().CurrentValue.ConfigureChannel, Is.SameAs(hook));
            Assert.That(sp.GetRequiredService<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>().CurrentValue.ConfigureChannel, Is.SameAs(hook));
            Assert.That(sp.GetRequiredService<IOptionsMonitor<GrpcSagaControlChannelOptions>>().CurrentValue.ConfigureChannel, Is.SameAs(hook));
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_registers_push_method_singleton()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc();

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
    public void AddLatticeReplicationGrpc_registers_snapshot_service_singleton()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc();

        using var sp = services.BuildServiceProvider();
        var first = sp.GetRequiredService<LatticeRemoteSnapshotGrpcService>();
        var second = sp.GetRequiredService<LatticeRemoteSnapshotGrpcService>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeReplicationGrpc_is_idempotent()
    {
        var services = BaseServices();
        services.AddLatticeReplicationGrpc(opts => opts.Peers["site-b"] = new Uri("https://localhost:5001/"));
        services.AddLatticeReplicationGrpc(opts => opts.AllowPlaintextEndpoints = true);

        using var sp = services.BuildServiceProvider();
        var unified = sp.GetRequiredService<IOptionsMonitor<LatticeReplicationGrpcOptions>>().CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(unified.Peers["site-b"], Is.EqualTo(new Uri("https://localhost:5001/")));
            Assert.That(unified.AllowPlaintextEndpoints, Is.True);
            // One push transport and one snapshot transport are registered, not two.
            Assert.That(services.Count(d => d.ServiceType == typeof(IReplicationTransport)), Is.EqualTo(1));
            Assert.That(services.Count(d => d.ServiceType == typeof(IRemoteSnapshotTransport)), Is.EqualTo(1));
        });
    }

    [Test]
    public void MapLatticeReplicationGrpc_throws_when_endpoints_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceCollectionExtensions.MapLatticeReplicationGrpc(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationGrpc_registers_default_Zstd_framing_compressor()
    {
        // Stand-alone gRPC hosts (no AddLatticeReplication call) must
        // still resolve a ZstdLatticeCompressor so the byte-keyed
        // compressor registry on the canonical encoder can decode
        // LatticeCompression.Zstd batches from a peer that does
        // compress. The fallback is registered via TryAddEnumerable
        // so co-existing with AddLatticeReplication's identical
        // fallback is order-insensitive and yields exactly one
        // registered instance.
        var services = BaseServices();

        services.AddLatticeReplicationGrpc();

        using var sp = services.BuildServiceProvider();
        var compressors = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(compressors, Has.Length.EqualTo(1));
            Assert.That(compressors[0], Is.InstanceOf<ZstdLatticeCompressor>());
            Assert.That(compressors[0].Algorithm, Is.EqualTo(LatticeCompression.Zstd));
        });
    }

    [Test]
    public void AddLatticeReplicationGrpc_preserves_pre_registered_Zstd_compressor_with_custom_level()
    {
        // Mirrors the replication-package precedent: a host that
        // pre-registers ZstdLatticeCompressor with a non-default
        // level must keep its instance after the gRPC fallback runs.
        var services = BaseServices();
        var custom = new ZstdLatticeCompressor(compressionLevel: 9);
        services.AddLatticeCompressor(custom);

        services.AddLatticeReplicationGrpc();

        using var sp = services.BuildServiceProvider();
        var compressors = sp.GetServices<ILatticeCompressor>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(compressors, Has.Length.EqualTo(1));
            Assert.That(compressors[0], Is.SameAs(custom));
        });
    }
}
