using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="LoopbackAwareSagaControlChannel"/>. Pins the routing
/// contract: a call for the local cluster is served in-process by
/// <see cref="ILatticeSagaControlHandler"/> (no gRPC endpoint required), and a call
/// for any other cluster is routed to the gRPC <see cref="GrpcSagaControlChannel"/>.
/// </summary>
[TestFixture]
public class LoopbackAwareSagaControlChannelTests
{
    private const string LocalCluster = "us";
    private const string RemoteCluster = "eu";

    private static LatticeSagaGrpcMethods CreateMethods()
    {
        var services = new ServiceCollection();
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

    private static GrpcSagaControlChannel CreateRemote(IOptionsMonitor<GrpcSagaControlChannelOptions> options)
    {
        var secrets = Substitute.For<IReplicationSecretProvider>();
        secrets.GetOutboundSecretAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>((string?)null));
        return new GrpcSagaControlChannel(CreateMethods(), options, secrets, NewReplicationOptions(LocalCluster));
    }

    private static SagaControlRequest Request() => new()
    {
        SagaId = "saga-1",
        TargetTree = "tree",
        CoordinatorClusterId = LocalCluster,
    };

    private static (LoopbackAwareSagaControlChannel Channel, ILatticeSagaControlHandler Handler) CreateChannel(
        string? localClusterId = LocalCluster,
        string fallbackClusterId = LocalCluster)
    {
        var options = NewOptions(new GrpcSagaControlChannelOptions { LocalClusterId = localClusterId });
        var handler = Substitute.For<ILatticeSagaControlHandler>();
        var vote = new SagaControlResponse { SagaId = "saga-1", Vote = SagaVote.Commit };
        handler.PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>()).Returns(vote);
        handler.CommitAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>()).Returns(vote);
        handler.AbortAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>()).Returns(vote);
        handler.GetStatusAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>()).Returns(vote);

        var channel = new LoopbackAwareSagaControlChannel(
            CreateRemote(options), handler, options, NewReplicationOptions(fallbackClusterId));
        return (channel, handler);
    }

    [Test]
    public void Constructor_throws_on_null_remote()
    {
        var options = NewOptions(new GrpcSagaControlChannelOptions());
        Assert.That(() => new LoopbackAwareSagaControlChannel(
            null!, Substitute.For<ILatticeSagaControlHandler>(), options, NewReplicationOptions(LocalCluster)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_local_handler()
    {
        var options = NewOptions(new GrpcSagaControlChannelOptions());
        Assert.That(() => new LoopbackAwareSagaControlChannel(
            CreateRemote(options), null!, options, NewReplicationOptions(LocalCluster)),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task PrepareAsync_routes_local_cluster_to_the_in_process_handler()
    {
        var (channel, handler) = CreateChannel();

        var response = await channel.PrepareAsync(LocalCluster, Request());

        Assert.That(response.Vote, Is.EqualTo(SagaVote.Commit));
        await handler.Received(1).PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task CommitAsync_and_AbortAsync_route_local_cluster_to_the_in_process_handler()
    {
        var (channel, handler) = CreateChannel();

        await channel.CommitAsync(LocalCluster, Request());
        await channel.AbortAsync(LocalCluster, Request());
        await channel.GetStatusAsync(LocalCluster, Request());

        await handler.Received(1).CommitAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
        await handler.Received(1).AbortAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
        await handler.Received(1).GetStatusAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void PrepareAsync_routes_a_remote_cluster_to_the_grpc_channel()
    {
        // With no peer endpoint configured, the remote gRPC leg raises its
        // "no endpoint configured" error - which proves the call was routed to the
        // remote channel rather than served in-process by the local handler.
        var (channel, handler) = CreateChannel();

        Assert.That(async () => await channel.PrepareAsync(RemoteCluster, Request()),
            Throws.InvalidOperationException.With.Message.Contains(RemoteCluster));
        handler.DidNotReceive().PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task IsLocal_falls_back_to_replication_cluster_id_when_local_id_is_unset()
    {
        // No explicit LocalClusterId on the saga options: the channel must fall back
        // to LatticeReplicationOptions.ClusterId to identify the local cluster.
        var (channel, handler) = CreateChannel(localClusterId: null, fallbackClusterId: LocalCluster);

        await channel.PrepareAsync(LocalCluster, Request());

        await handler.Received(1).PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>());
    }
}
