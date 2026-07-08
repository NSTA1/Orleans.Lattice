using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="GrpcSagaControlChannel"/> and
/// <see cref="PeerMapSagaPeerAuthorizer"/>. Pins the constructor guards,
/// argument validation, configuration-lookup errors, disposal
/// idempotency, and the default peer-authorization policy that does not
/// require a live channel.
/// </summary>
[TestFixture]
public class GrpcSagaControlChannelTests
{
    private static LatticeSagaGrpcMethods CreateMethods()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSerializer();
        var sp = services.BuildServiceProvider();
        return new LatticeSagaGrpcMethods(
            sp.GetRequiredService<Serializer<SagaControlRequest>>(),
            sp.GetRequiredService<Serializer<SagaControlResponse>>());
    }

    private static IOptionsMonitor<GrpcSagaControlChannelOptions> NewOptions(GrpcSagaControlChannelOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<GrpcSagaControlChannelOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static IOptionsMonitor<LatticeReplicationOptions> NewReplicationOptions(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    private static GrpcSagaControlChannel CreateChannel(GrpcSagaControlChannelOptions options)
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        return new GrpcSagaControlChannel(CreateMethods(), NewOptions(options), secrets, NewReplicationOptions("local"));
    }

    private static SagaControlRequest Request() => new()
    {
        SagaId = "saga-1",
        TargetTree = "tree",
        CoordinatorClusterId = "local",
    };

    [Test]
    public void Constructor_throws_on_null_methods()
    {
        Assert.That(() => new GrpcSagaControlChannel(
            null!, NewOptions(new GrpcSagaControlChannelOptions()),
            Substitute.For<IReplicationSecretProvider>(), NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(() => new GrpcSagaControlChannel(
            CreateMethods(), null!,
            Substitute.For<IReplicationSecretProvider>(), NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_secrets()
    {
        Assert.That(() => new GrpcSagaControlChannel(
            CreateMethods(), NewOptions(new GrpcSagaControlChannelOptions()),
            null!, NewReplicationOptions("local")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_replication_options()
    {
        Assert.That(() => new GrpcSagaControlChannel(
            CreateMethods(), NewOptions(new GrpcSagaControlChannelOptions()),
            Substitute.For<IReplicationSecretProvider>(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void PrepareAsync_throws_when_cluster_id_is_whitespace()
    {
        using var channel = CreateChannel(new GrpcSagaControlChannelOptions());
        Assert.That(async () => await channel.PrepareAsync("  ", Request()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void CommitAsync_throws_when_cluster_has_no_endpoint()
    {
        using var channel = CreateChannel(new GrpcSagaControlChannelOptions());
        Assert.That(async () => await channel.CommitAsync("site-a", Request()),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contain("site-a"));
    }

    [Test]
    public void Plaintext_endpoint_is_rejected_by_default()
    {
        var options = new GrpcSagaControlChannelOptions();
        options.PeerEndpoints["site-a"] = new Uri("http://saga.site-a.example/");
        using var channel = CreateChannel(options);
        Assert.That(async () => await channel.AbortAsync("site-a", Request()),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetStatusAsync_throws_after_disposal()
    {
        var channel = CreateChannel(new GrpcSagaControlChannelOptions());
        channel.Dispose();
        Assert.That(async () => await channel.GetStatusAsync("site-a", Request()),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var channel = CreateChannel(new GrpcSagaControlChannelOptions());
        channel.Dispose();
        Assert.That(() => channel.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Authorizer_constructor_throws_on_null_options()
    {
        Assert.That(() => new PeerMapSagaPeerAuthorizer(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Authorizer_authorizes_configured_peer()
    {
        var options = new LatticeReplicationGrpcOptions();
        options.Peers["site-a"] = new Uri("https://site-a.example/");
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        var authorizer = new PeerMapSagaPeerAuthorizer(monitor);

        Assert.That(await authorizer.IsAuthorizedAsync("site-a"), Is.True);
    }

    [Test]
    public async Task Authorizer_rejects_unknown_and_empty_origin()
    {
        var options = new LatticeReplicationGrpcOptions();
        options.Peers["site-a"] = new Uri("https://site-a.example/");
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        var authorizer = new PeerMapSagaPeerAuthorizer(monitor);

        Assert.That(await authorizer.IsAuthorizedAsync("rogue"), Is.False);
        Assert.That(await authorizer.IsAuthorizedAsync(null), Is.False);
        Assert.That(await authorizer.IsAuthorizedAsync("  "), Is.False);
    }
}
