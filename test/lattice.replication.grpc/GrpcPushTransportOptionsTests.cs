using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
public class GrpcPushTransportOptionsTests
{
    [Test]
    public void PeerEndpoints_defaults_to_empty_ordinal_dictionary()
    {
        var options = new GrpcPushTransportOptions();

        Assert.That(options.PeerEndpoints, Is.Not.Null);
        Assert.That(options.PeerEndpoints, Is.Empty);
    }

    [Test]
    public void ConfigureChannel_defaults_to_null()
    {
        var options = new GrpcPushTransportOptions();

        Assert.That(options.ConfigureChannel, Is.Null);
    }

    [Test]
    public void PeerEndpoints_uses_ordinal_string_comparer()
    {
        var options = new GrpcPushTransportOptions();
        options.PeerEndpoints["Peer"] = new Uri("https://example/");

        // Ordinal comparison: "peer" != "Peer".
        Assert.That(options.PeerEndpoints.ContainsKey("peer"), Is.False);
        Assert.That(options.PeerEndpoints.ContainsKey("Peer"), Is.True);
    }

    [Test]
    public void ConfigureChannel_can_be_assigned_and_invoked()
    {
        var options = new GrpcPushTransportOptions();
        var captured = string.Empty;
        options.ConfigureChannel = (id, _) => captured = id;

        Assert.That(options.ConfigureChannel, Is.Not.Null);
        options.ConfigureChannel!("peer-1", new global::Grpc.Net.Client.GrpcChannelOptions());
        Assert.That(captured, Is.EqualTo("peer-1"));
    }
}
