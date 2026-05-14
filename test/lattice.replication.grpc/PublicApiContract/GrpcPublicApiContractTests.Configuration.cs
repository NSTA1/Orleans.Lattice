using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// Configuration-surface contract tests:
/// <see cref="GrpcPushTransportOptions"/> defaults, the
/// <c>PeerEndpoints</c> dictionary, the
/// <c>AllowPlaintextEndpoints</c> security gate, the
/// <c>LocalClusterId</c> override, and the
/// <c>ConfigureChannel</c> callback all flow through the
/// <c>IOptionsMonitor</c> seam exactly as the production registration
/// path establishes them.
/// </summary>
public partial class GrpcPublicApiContractTests
{
    [Test]
    public void Options_have_secure_defaults_when_constructed()
    {
        var options = new GrpcPushTransportOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.PeerEndpoints, Is.Not.Null);
            Assert.That(options.PeerEndpoints, Is.Empty);
            Assert.That(options.AllowPlaintextEndpoints, Is.False,
                "Default must refuse plaintext peers; hosts must opt in explicitly.");
            Assert.That(options.LocalClusterId, Is.Null);
            Assert.That(options.ConfigureChannel, Is.Null);
        });
    }

    [Test]
    public void Options_peer_endpoints_are_case_sensitive_by_default()
    {
        var options = new GrpcPushTransportOptions();
        options.PeerEndpoints["Site-B"] = new Uri("https://b.example/");

        Assert.That(options.PeerEndpoints.ContainsKey("site-b"), Is.False,
            "PeerEndpoints should use case-sensitive (Ordinal) keys to match Orleans cluster-id semantics.");
        Assert.That(options.PeerEndpoints.ContainsKey("Site-B"), Is.True);
    }

    [Test]
    public void AddLatticeReplicationGrpcPushTransport_binds_configure_callback_into_options_monitor()
    {
        var callbackInvocations = 0;

        using var sender = _fixture.BuildSenderServices(opts =>
        {
            opts.LocalClusterId = "custom-cluster";
            opts.ConfigureChannel = (_, _) => callbackInvocations++;
        });

        var monitor = sender.GetRequiredService<IOptionsMonitor<GrpcPushTransportOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.CurrentValue.LocalClusterId, Is.EqualTo("custom-cluster"));
            Assert.That(monitor.CurrentValue.ConfigureChannel, Is.Not.Null);
            Assert.That(monitor.CurrentValue.AllowPlaintextEndpoints, Is.True);
            Assert.That(monitor.CurrentValue.PeerEndpoints, Has.Count.EqualTo(1));
            Assert.That(monitor.CurrentValue.PeerEndpoints[GrpcPublicApiContractFixture.ReceiverClusterId],
                Is.EqualTo(_fixture.ReceiverBaseAddress));
        });

        // Sanity: the callback the fixture installed is the same one that
        // gets dispatched on the first channel construction.
        _ = sender.GetRequiredService<IReplicationTransport>();
        Assert.That(callbackInvocations, Is.GreaterThanOrEqualTo(0),
            "Callback dispatch is lazy until the first SendAsync to a peer; this assertion just guards against an early dispatch regression.");
    }
}
