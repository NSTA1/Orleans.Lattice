using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests.PeerStatus;

/// <summary>
/// Full-pipeline tests that map the peer-status service on a real
/// (<see cref="TestServer"/>-hosted) ASP.NET Core gRPC endpoint alongside the
/// control service and drive it through the public client: an opted-in host
/// serves a page end to end, and a host left on the default-deny authorizer
/// rejects the call with <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class LatticeReplicationStatusGrpcEndToEndTests
{
    private static async Task<IHost> StartHostAsync(ILatticeReplicationStatus status, bool allowAll) =>
        await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddSingleton(status);
                    services.AddSingleton(Substitute.For<ILatticeReplicationControl>());
                    if (allowAll)
                    {
                        services.AddSingleton<ILatticeReplicationApiAuthorizer, AllowAllReplicationApiAuthorizer>();
                    }

                    services.AddLatticeReplicationApiGrpc();
                    services.AddLatticeReplicationStatusApiGrpc();
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapLatticeReplicationApiGrpc();
                        endpoints.MapLatticeReplicationStatusApiGrpc();
                    });
                });
            })
            .StartAsync();

    private static LatticeReplicationStatusGrpcClient CreateClient(IHost host, out GrpcChannel channel)
    {
        var testServer = host.GetTestServer();
        channel = GrpcChannel.ForAddress(
            testServer.BaseAddress,
            new GrpcChannelOptions { HttpHandler = testServer.CreateHandler() });
        return LatticeReplicationStatusGrpcClient.Create(channel.CreateCallInvoker(), host.Services);
    }

    [Test]
    public async Task MapLatticeReplicationStatusApiGrpc_serves_a_page_end_to_end()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationPeerStatusPage(
                "west",
                new[]
                {
                    new ReplicationPeerStatusEntry(
                        "a/crm/contacts", "east", ReplicationLinkDirection.Outbound,
                        0, 0, 0, TimeSpan.FromSeconds(1), 0, ReplicationLinkHealth.Healthy),
                },
                continuationToken: null));
        using var host = await StartHostAsync(status, allowAll: true);
        var client = CreateClient(host, out var channel);
        using var _ = channel;

        var page = await client.GetPeerStatusAsync(new ReplicationPeerStatusQuery { PageSize = 10 });

        Assert.Multiple(() =>
        {
            Assert.That(page.LocalRegionId, Is.EqualTo("west"));
            Assert.That(page.Peers.Single().TreeId, Is.EqualTo("a/crm/contacts"));
            Assert.That(page.Peers.Single().Health, Is.EqualTo(ReplicationLinkHealth.Healthy));
        });

        await host.StopAsync();
    }

    [Test]
    public async Task MapLatticeReplicationStatusApiGrpc_is_default_deny()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        using var host = await StartHostAsync(status, allowAll: false);
        var client = CreateClient(host, out var channel);
        using var _ = channel;

        var ex = Assert.ThrowsAsync<RpcException>(async () => await client.GetPeerStatusAsync(ReplicationPeerStatusQuery.All));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        await status.DidNotReceiveWithAnyArgs().GetPeerStatusAsync(default!, default);

        await host.StopAsync();
    }
}
