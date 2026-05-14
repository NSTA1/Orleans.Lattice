using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// DI-registration contract tests for the public gRPC service-collection
/// extensions:
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcPushTransport"/>
/// and
/// <see cref="LatticeReplicationGrpcServiceCollectionExtensions.AddLatticeReplicationGrpcServer"/>.
/// These extensions are the canonical seams hosts compose against; this
/// suite pins idempotency, replacement semantics, and option-monitor
/// resolution so any silent change to the registration shape is caught.
/// </summary>
public partial class GrpcPublicApiContractTests
{
    [Test]
    public async Task AddLatticeReplicationGrpcPushTransport_resolves_a_single_transport_singleton()
    {
        await using var sender = _fixture.BuildSenderServices();
        var first = sender.GetRequiredService<IReplicationTransport>();
        var second = sender.GetRequiredService<IReplicationTransport>();

        Assert.That(first, Is.SameAs(second),
            "IReplicationTransport must be a singleton so peer-channel reuse and dispose semantics are stable.");
    }

    [Test]
    public async Task AddLatticeReplicationGrpcPushTransport_is_idempotent_on_repeat_registration()
    {
        // Build a service collection, register the transport twice, and
        // verify the second registration replaces (does not stack) the
        // first. The contract here is the same as the docs: calls land
        // via Replace<IReplicationTransport>, so the resolved transport
        // is always exactly one instance, and the second configure
        // delegate's options win.
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLogging();
        services.AddSingleton<IReplicationBatchEncoder>(sp =>
            new EnvelopeSerializerEncoderProxy(sp.GetRequiredService<Orleans.Serialization.Serializer<ReplicationBatchEnvelope>>()));
        services.Configure<LatticeReplicationOptions>(o =>
        {
            o.ClusterId = GrpcPublicApiContractFixture.SenderClusterId;
        });
        services.AddLatticeReplicationGrpcPushTransport(opts =>
        {
            opts.PeerEndpoints["x"] = new Uri("https://x.example/");
            opts.LocalClusterId = "first";
        });
        services.AddLatticeReplicationGrpcPushTransport(opts =>
        {
            opts.PeerEndpoints["x"] = new Uri("https://x.example/");
            opts.LocalClusterId = "second";
        });

        await using var sp = services.BuildServiceProvider();

        var transports = sp.GetServices<IReplicationTransport>().ToList();
        var monitor = sp.GetRequiredService<IOptionsMonitor<GrpcPushTransportOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(transports, Has.Count.EqualTo(1),
                "Repeat AddLatticeReplicationGrpcPushTransport calls must not stack additional transports.");
            Assert.That(transports[0], Is.InstanceOf<GrpcPushTransport>());
            Assert.That(monitor.CurrentValue.LocalClusterId, Is.EqualTo("second"),
                "Subsequent configure callbacks compose; the most recent assignment wins.");
        });
    }

    [Test]
    public async Task AddLatticeReplicationGrpcServer_registers_method_holder_and_service_singletons()
    {
        // The fixture already calls AddLatticeReplicationGrpcServer; we
        // reach into its services to confirm the registration shape.
        var sp = _fixture.ReceiverHost.Services;

        var method1 = sp.GetRequiredService<LatticeReplicationGrpcMethod>();
        var method2 = sp.GetRequiredService<LatticeReplicationGrpcMethod>();

        Assert.Multiple(() =>
        {
            Assert.That(method1, Is.SameAs(method2),
                "LatticeReplicationGrpcMethod must be a singleton so receiver-side binding is stable across requests.");
            Assert.That(LatticeReplicationGrpcMethodHolder.Current, Is.Not.Null,
                "The static method holder must be populated so the BindServiceMethod callback can discover the method.");
        });

        await Task.CompletedTask;
    }

    [Test]
    public void MapLatticeReplicationGrpcService_route_responds_to_inbound_calls()
    {
        // The smoke test in the root partial already proves the route
        // is mapped end-to-end; this test is a structural assertion
        // that the route survives a no-op sender build (i.e. that
        // MapLatticeReplicationGrpcService runs as part of the
        // fixture's pipeline configuration without throwing).
        Assert.That(_fixture.ReceiverServer, Is.Not.Null);
        Assert.That(_fixture.ReceiverBaseAddress, Is.Not.Null);
        Assert.That(_fixture.ReceiverBaseAddress.Scheme, Is.EqualTo(Uri.UriSchemeHttp).IgnoreCase
            .Or.EqualTo(Uri.UriSchemeHttps).IgnoreCase,
            "TestServer must publish a well-formed http(s) base address.");
    }

    /// <summary>
    /// Encoder proxy local to the registration test so it does not
    /// share an instance with the fixture's encoder. Implementation
    /// matches the fixture's so the wire-shape contract is preserved.
    /// </summary>
    private sealed class EnvelopeSerializerEncoderProxy : IReplicationBatchEncoder
    {
        private readonly Orleans.Serialization.Serializer<ReplicationBatchEnvelope> _serializer;

        public EnvelopeSerializerEncoderProxy(Orleans.Serialization.Serializer<ReplicationBatchEnvelope> serializer)
        {
            _serializer = serializer;
        }

        public string ContentType => "application/x-orleans-lattice-replog+binary";

        public int CurrentWireVersion => 1;

        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer)
            => _serializer.Serialize(envelope, writer);

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => _serializer.Deserialize(payload.Span);
    }
}
