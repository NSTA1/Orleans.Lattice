using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// Configuration-surface contract tests:
/// <see cref="LatticeReplicationGrpcOptions"/> defaults, the
/// <c>Peers</c> dictionary, the
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
        var options = new LatticeReplicationGrpcOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Peers, Is.Not.Null);
            Assert.That(options.Peers, Is.Empty);
            Assert.That(options.AllowPlaintextEndpoints, Is.False,
                "Default must refuse plaintext peers; hosts must opt in explicitly.");
            Assert.That(options.LocalClusterId, Is.Null);
            Assert.That(options.ConfigureChannel, Is.Null);
        });
    }

    [Test]
    public void Options_peers_are_case_sensitive_by_default()
    {
        var options = new LatticeReplicationGrpcOptions();
        options.Peers["Site-B"] = new Uri("https://b.example/");

        Assert.That(options.Peers.ContainsKey("site-b"), Is.False,
            "Peers should use case-sensitive (Ordinal) keys to match Orleans cluster-id semantics.");
        Assert.That(options.Peers.ContainsKey("Site-B"), Is.True);
    }

    [Test]
    public void AddLatticeReplicationGrpc_binds_configure_callback_into_options_monitor()
    {
        var callbackInvocations = 0;

        using var sender = _fixture.BuildSenderServices(opts =>
        {
            opts.LocalClusterId = "custom-cluster";
            opts.ConfigureChannel = (_, _) => callbackInvocations++;
        });

        var monitor = sender.GetRequiredService<IOptionsMonitor<LatticeReplicationGrpcOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.CurrentValue.LocalClusterId, Is.EqualTo("custom-cluster"));
            Assert.That(monitor.CurrentValue.ConfigureChannel, Is.Not.Null);
            Assert.That(monitor.CurrentValue.AllowPlaintextEndpoints, Is.True);
            Assert.That(monitor.CurrentValue.Peers, Has.Count.EqualTo(1));
            Assert.That(monitor.CurrentValue.Peers[GrpcPublicApiContractFixture.ReceiverClusterId],
                Is.EqualTo(_fixture.ReceiverBaseAddress));
        });

        // Sanity: the callback the fixture installed is the same one that
        // gets dispatched on the first channel construction, and resolving
        // the transport alone must not construct that channel eagerly.
        _ = sender.GetRequiredService<IReplicationTransport>();
        Assert.That(callbackInvocations, Is.Zero,
            "Callback dispatch is lazy until the first SendAsync to a peer.");
    }
}
