using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Validates the default state of
/// <see cref="GrpcRemoteSnapshotTransportOptions"/>. Pins the
/// receiver-side defaults the transport composes against: an empty
/// endpoint map, secure-only channels by default, no channel-config
/// hook, and a local-cluster-id override that defers to
/// <see cref="LatticeReplicationOptions.ClusterId"/>.
/// </summary>
[TestFixture]
public class GrpcRemoteSnapshotTransportOptionsTests
{
    [Test]
    public void Defaults_match_documented_baseline()
    {
        var options = new GrpcRemoteSnapshotTransportOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.SenderEndpoints, Is.Not.Null);
            Assert.That(options.SenderEndpoints, Is.Empty);
            Assert.That(options.AllowPlaintextEndpoints, Is.False);
            Assert.That(options.ConfigureChannel, Is.Null);
            Assert.That(options.LocalClusterId, Is.Null);
        });
    }

    [Test]
    public void SenderEndpoints_accepts_ordinal_keyed_entries()
    {
        var options = new GrpcRemoteSnapshotTransportOptions();
        options.SenderEndpoints["site-a"] = new Uri("https://snap.site-a.example/");

        Assert.That(options.SenderEndpoints["site-a"], Is.EqualTo(new Uri("https://snap.site-a.example/")));
    }

    [Test]
    public void SenderEndpoints_is_case_sensitive_by_default()
    {
        var options = new GrpcRemoteSnapshotTransportOptions();
        options.SenderEndpoints["site-a"] = new Uri("https://snap.site-a.example/");

        Assert.That(options.SenderEndpoints.ContainsKey("SITE-A"), Is.False);
    }

    [Test]
    public void ConfigureChannel_hook_round_trips_assigned_delegate()
    {
        var options = new GrpcRemoteSnapshotTransportOptions();
        Action<string, global::Grpc.Net.Client.GrpcChannelOptions> hook = (_, _) => { };
        options.ConfigureChannel = hook;

        Assert.That(options.ConfigureChannel, Is.SameAs(hook));
    }
}