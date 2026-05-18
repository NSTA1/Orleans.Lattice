using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcRemoteSnapshotTransport"/> focused on
/// constructor guards, argument validation, configuration lookup
/// errors, and disposal idempotency. The full wire-level behaviour
/// (metadata round-trip, stream draining, point-in-time consistency,
/// cancellation under load) is covered by
/// <see cref="GrpcRemoteSnapshotTransportContractTests"/>; this fixture
/// pins the synchronous failure modes that do not require a live
/// channel.
/// </summary>
[TestFixture]
public class GrpcRemoteSnapshotTransportTests
{
    private static LatticeRemoteSnapshotGrpcMethods CreateMethods()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();

        return new LatticeRemoteSnapshotGrpcMethods(
            sp.GetRequiredService<Orleans.Serialization.Serializer<RemoteSnapshotMetadataRequest>>(),
            sp.GetRequiredService<Orleans.Serialization.Serializer<RemoteSnapshotMetadata>>(),
            sp.GetRequiredService<Orleans.Serialization.Serializer<RemoteSnapshotStreamItem>>());
    }

    private static IOptionsMonitor<GrpcRemoteSnapshotTransportOptions> NewOptions(GrpcRemoteSnapshotTransportOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<GrpcRemoteSnapshotTransportOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> NewReplicationOptions(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    private static GrpcRemoteSnapshotTransport CreateTransport(GrpcRemoteSnapshotTransportOptions options)
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        return new GrpcRemoteSnapshotTransport(
            CreateMethods(),
            NewOptions(options),
            secrets,
            NewReplicationOptions("local"));
    }

    [Test]
    public void Constructor_throws_on_null_methods()
    {
        Assert.That(() => new GrpcRemoteSnapshotTransport(
                null!,
                NewOptions(new GrpcRemoteSnapshotTransportOptions()),
                Substitute.For<IReplicationSecretProvider>(),
                NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(() => new GrpcRemoteSnapshotTransport(
                CreateMethods(),
                null!,
                Substitute.For<IReplicationSecretProvider>(),
                NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_secrets()
    {
        Assert.That(() => new GrpcRemoteSnapshotTransport(
                CreateMethods(),
                NewOptions(new GrpcRemoteSnapshotTransportOptions()),
                null!,
                NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_replication_options()
    {
        Assert.That(() => new GrpcRemoteSnapshotTransport(
                CreateMethods(),
                NewOptions(new GrpcRemoteSnapshotTransportOptions()),
                Substitute.For<IReplicationSecretProvider>(),
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetMetadataAsync_throws_when_tree_name_is_whitespace()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () => await transport.GetMetadataAsync("   ", "site-a", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_source_cluster_id_is_whitespace()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () => await transport.GetMetadataAsync("tree", "  ", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMetadataAsync_throws_when_source_cluster_has_no_endpoint()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () => await transport.GetMetadataAsync("tree", "site-a", HybridLogicalClock.Zero),
            Throws.InstanceOf<InvalidOperationException>()
                  .With.Message.Contain("site-a"));
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_tree_name_is_whitespace()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () =>
        {
            await foreach (var _ in transport.RequestSnapshotAsync(" ", "site-a", HybridLogicalClock.Zero))
            {
            }
        }, Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_source_cluster_id_is_whitespace()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () =>
        {
            await foreach (var _ in transport.RequestSnapshotAsync("tree", " ", HybridLogicalClock.Zero))
            {
            }
        }, Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_when_source_cluster_has_no_endpoint()
    {
        using var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        Assert.That(async () =>
        {
            await foreach (var _ in transport.RequestSnapshotAsync("tree", "site-a", HybridLogicalClock.Zero))
            {
            }
        }, Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contain("site-a"));
    }

    [Test]
    public void Plaintext_endpoint_is_rejected_by_default()
    {
        var options = new GrpcRemoteSnapshotTransportOptions();
        options.SenderEndpoints["site-a"] = new Uri("http://snap.site-a.example/");

        using var transport = CreateTransport(options);
        Assert.That(async () => await transport.GetMetadataAsync("tree", "site-a", HybridLogicalClock.Zero),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetMetadataAsync_throws_after_disposal()
    {
        var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        transport.Dispose();

        Assert.That(async () => await transport.GetMetadataAsync("tree", "site-a", HybridLogicalClock.Zero),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void RequestSnapshotAsync_throws_after_disposal()
    {
        var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        transport.Dispose();

        Assert.That(async () =>
        {
            await foreach (var _ in transport.RequestSnapshotAsync("tree", "site-a", HybridLogicalClock.Zero))
            {
            }
        }, Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var transport = CreateTransport(new GrpcRemoteSnapshotTransportOptions());
        transport.Dispose();
        Assert.That(() => transport.Dispose(), Throws.Nothing);
    }
}