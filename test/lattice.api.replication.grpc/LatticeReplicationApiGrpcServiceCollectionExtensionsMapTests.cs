using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Serialization;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Tests for the endpoint-mapping half of
/// <see cref="LatticeReplicationApiGrpcServiceCollectionExtensions"/>: the
/// argument guard on <c>MapLatticeReplicationApiGrpc</c>, and an in-memory,
/// full-pipeline integration test that maps the service on a real
/// (<see cref="TestServer"/>-hosted) ASP.NET Core gRPC endpoint and drives an RPC
/// through the strongly-typed client end to end.
/// </summary>
public sealed class LatticeReplicationApiGrpcServiceCollectionExtensionsMapTests
{
    [Test]
    public void MapLatticeReplicationApiGrpc_null_endpoints_throws() =>
        Assert.Throws<ArgumentNullException>(
            () => LatticeReplicationApiGrpcServiceCollectionExtensions.MapLatticeReplicationApiGrpc(null!));

    [Test]
    [Category("Integration")]
    public async Task MapLatticeReplicationApiGrpc_serves_a_mapped_rpc_end_to_end()
    {
        var control = Substitute.For<ILatticeReplicationControl>();
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, mode: LatticeMergeMode.RwFlag, ambiguous: false),
            }));

        using var host = await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddSingleton(control);
                    // Opt in to a permissive authorizer before the binding registers
                    // the default-deny one, so the mapped RPC is reachable.
                    services.AddSingleton<ILatticeReplicationApiAuthorizer, AllowAllReplicationApiAuthorizer>();
                    services.AddLatticeReplicationApiGrpc();
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeReplicationApiGrpc());
                });
            })
            .StartAsync();

        var testServer = host.GetTestServer();
        using var channel = GrpcChannel.ForAddress(
            testServer.BaseAddress,
            new GrpcChannelOptions { HttpHandler = testServer.CreateHandler() });
        var client = LatticeReplicationApiGrpcClient.Create(channel.CreateCallInvoker(), host.Services);

        var report = await client.GetReplicationConfigAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.Trees, Has.Count.EqualTo(1));
            Assert.That(report.Trees[0].TreeId, Is.EqualTo("orders"));
            Assert.That(report.Trees[0].Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
        });

        await host.StopAsync();
    }
}
