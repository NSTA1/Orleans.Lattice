using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the endpoint-mapping half of the binding. Maps the
/// tenant-administration service on a real, in-memory
/// <see cref="TestServer"/>-hosted ASP.NET Core gRPC pipeline and drives RPCs
/// through the public strongly-typed clients, so the whole composition path -
/// <c>AddLatticeTenantAdminApiGrpc</c>, the method-definition factory that
/// publishes the static holder, <c>MapLatticeTenantAdminApiGrpc</c>, the
/// registered auth interceptor, and <c>Client.Create</c> - is exercised together
/// rather than in isolation. Also proves the default-deny posture holds over a
/// real transport.
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public sealed class LatticeTenantAdminApiGrpcMapRoutingTests
{
    private static async Task<IHost> StartHostAsync(
        ILatticeTenantAdmin admin,
        ILatticeTenantSelfService selfService,
        bool permissive)
    {
        var host = await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddSingleton(admin);
                    services.AddSingleton(selfService);
                    if (permissive)
                    {
                        // Opt in before the binding TryAdds the default-deny
                        // authorizer, so the mapped lifecycle RPCs are reachable.
                        services.AddSingleton<ILatticeTenantAdminApiAuthorizer, AllowAllTenantAdminApiAuthorizer>();
                    }

                    services.AddLatticeTenantAdminApiGrpc(o =>
                        o.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor
                        {
                            SchemeId = "basic",
                            DisplayName = "Basic",
                        }));
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapLatticeTenantAdminApiGrpc());
                });
            })
            .StartAsync();
        return host;
    }

    private static (GrpcChannel Channel, CallInvoker Invoker) Connect(IHost host)
    {
        var testServer = host.GetTestServer();
        var channel = GrpcChannel.ForAddress(
            testServer.BaseAddress,
            new GrpcChannelOptions { HttpHandler = testServer.CreateHandler() });
        return (channel, channel.CreateCallInvoker());
    }

    [Test]
    public async Task A_mapped_lifecycle_rpc_is_served_end_to_end()
    {
        var facade = new FakeTenantAdmin();
        using var host = await StartHostAsync(facade, new FakeTenantSelfService(), permissive: true);
        var (channel, invoker) = Connect(host);
        using (channel)
        {
            var client = LatticeTenantAdminApiGrpcClient.Create(invoker, host.Services);

            var result = await client.CreateTenantAsync("acme", ["ops@example.com"]);

            Assert.Multiple(() =>
            {
                Assert.That(result.TenantId, Is.EqualTo("acme"));
                Assert.That(result.Status, Is.EqualTo(TenantLifecycleStatus.Active));
                Assert.That(facade.LastTenantId, Is.EqualTo("acme"));
                Assert.That(facade.LastAdminSubjects, Is.EqualTo(new[] { "ops@example.com" }));
            });
        }

        await host.StopAsync();
    }

    [Test]
    public async Task A_mapped_self_service_rpc_is_served_end_to_end()
    {
        using var host = await StartHostAsync(new FakeTenantAdmin(), new FakeTenantSelfService(), permissive: false);
        var (channel, invoker) = Connect(host);
        using (channel)
        {
            var client = LatticeTenantSelfServiceApiGrpcClient.Create(invoker, host.Services);

            var tenants = await client.ListAccessibleTenantsAsync();

            Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "acme", "beta" }),
                "the read-only self-service surface is exempt from the default-deny admin gate");
        }

        await host.StopAsync();
    }

    [Test]
    public async Task The_unauthenticated_auth_scheme_rpc_is_reachable_without_opting_in()
    {
        using var host = await StartHostAsync(new FakeTenantAdmin(), new FakeTenantSelfService(), permissive: false);
        var (channel, invoker) = Connect(host);
        using (channel)
        {
            var client = LatticeTenantAdminApiGrpcClient.Create(invoker, host.Services);

            var schemes = await client.GetAuthSchemeAsync();

            Assert.That(schemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "basic" }));
        }

        await host.StopAsync();
    }

    [Test]
    public async Task A_lifecycle_rpc_is_denied_over_the_wire_when_the_host_never_opts_in()
    {
        var facade = new FakeTenantAdmin();
        using var host = await StartHostAsync(facade, new FakeTenantSelfService(), permissive: false);
        var (channel, invoker) = Connect(host);
        using (channel)
        {
            var client = LatticeTenantAdminApiGrpcClient.Create(invoker, host.Services);

            var ex = Assert.ThrowsAsync<RpcException>(async () => await client.DeleteTenantAsync("acme"));

            Assert.Multiple(() =>
            {
                Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
                Assert.That(facade.LastTenantId, Is.Null, "a denied call must never reach the facade");
            });
        }

        await host.StopAsync();
    }
}
