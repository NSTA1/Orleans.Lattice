using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Proves <see cref="LatticeAuthApiGrpcServiceCollectionExtensions.MapLatticeAuthApiGrpc"/>
/// wires the auth-API routes and the authorization interceptor into an ASP.NET
/// Core endpoint pipeline. Uses a substitute facade (the call never reaches it):
/// with the binding's default-deny meta-authorizer, a mapped call is rejected at
/// the interceptor with <see cref="StatusCode.PermissionDenied"/>, which
/// simultaneously proves the route exists (a missing route would surface
/// <see cref="StatusCode.Unimplemented"/>) and that the interceptor is engaged.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeAuthApiGrpcMapRoutingTests
{
    private IHost _host = null!;
    private GrpcChannel _channel = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddSerializer();
                    services.AddLogging();
                    services.AddRouting();
                    services.AddSingleton(Substitute.For<ILatticeAuthAdmin>());
                    services.AddLatticeAuthApiGrpc(o => o.RequireAuthorization = true);
                });
                web.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(e => e.MapLatticeAuthApiGrpc());
                });
            })
            .StartAsync();

        var server = _host.GetTestServer();
        _channel = GrpcChannel.ForAddress(server.BaseAddress, new GrpcChannelOptions
        {
            HttpHandler = server.CreateHandler(),
        });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _channel?.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync();
            _host.Dispose();
        }
    }

    [Test]
    public async Task Mapped_route_is_reachable_and_guarded_by_the_interceptor()
    {
        var methods = _host.Services.GetRequiredService<LatticeAuthApiGrpcMethods>();
        var invoker = _channel.CreateCallInvoker();

        using var call = invoker.AsyncUnaryCall(
            methods.GetGroup,
            host: null,
            new CallOptions(),
            new AuthGroupRef { GroupId = "anyone" });

        var ex = Assert.ThrowsAsync<RpcException>(async () => await call.ResponseAsync);

        Assert.That(
            ex!.StatusCode,
            Is.EqualTo(StatusCode.PermissionDenied),
            "A mapped route exists (not Unimplemented) and the default-deny interceptor guards it.");
    }
}
