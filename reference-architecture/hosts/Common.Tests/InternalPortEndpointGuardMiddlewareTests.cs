using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

namespace Orleans.Lattice.ReferenceArchitecture.Hosting.Tests;

/// <summary>
/// End-to-end coverage of <see cref="InternalPortEndpointGuardApplicationBuilderExtensions.UseInternalPortEndpointGuard"/>
/// and <see cref="InternalPortEndpointGuardMiddleware"/>. The middleware discriminates on the connection's local
/// port, so requests are driven through <see cref="TestServer.SendAsync(System.Action{HttpContext}, System.Threading.CancellationToken)"/>
/// with the local port set explicitly (the in-memory test server opens no real sockets).
/// </summary>
[TestFixture]
public sealed class InternalPortEndpointGuardMiddlewareTests
{
    // The silo's two Kestrel listeners: an internal HTTP/1 port and an externally
    // exposed HTTP/2 port.
    private const int InternalPort = 8080;
    private const int ExternalPort = 8081;

    private static async Task<IHost> CreateHostAsync(int internalPort, params string[] guardedPrefixes)
    {
        return await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.Configure(app =>
                {
                    app.UseInternalPortEndpointGuard(internalPort, guardedPrefixes);
                    app.Run(async context =>
                    {
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsync("ok");
                    });
                });
            })
            .StartAsync();
    }

    private static async Task<int> StatusOnPortAsync(IHost host, string path, int localPort)
    {
        var context = await host.GetTestServer().SendAsync(ctx =>
        {
            ctx.Request.Method = HttpMethods.Get;
            ctx.Request.Path = path;
            ctx.Connection.LocalPort = localPort;
        });

        return context.Response.StatusCode;
    }

    [Test]
    public async Task A_guarded_path_on_the_internal_port_is_served()
    {
        using var host = await CreateHostAsync(InternalPort, "/metrics", "/lattice/scale");

        var metrics = await StatusOnPortAsync(host, "/metrics", InternalPort);
        var scale = await StatusOnPortAsync(host, "/lattice/scale", InternalPort);

        Assert.Multiple(() =>
        {
            Assert.That(metrics, Is.EqualTo((int)HttpStatusCode.OK));
            Assert.That(scale, Is.EqualTo((int)HttpStatusCode.OK));
        });
    }

    [Test]
    public async Task A_guarded_path_on_the_external_port_returns_404()
    {
        using var host = await CreateHostAsync(InternalPort, "/metrics", "/lattice/scale");

        var metrics = await StatusOnPortAsync(host, "/metrics", ExternalPort);
        var scale = await StatusOnPortAsync(host, "/lattice/scale", ExternalPort);

        Assert.Multiple(() =>
        {
            Assert.That(metrics, Is.EqualTo((int)HttpStatusCode.NotFound));
            Assert.That(scale, Is.EqualTo((int)HttpStatusCode.NotFound));
        });
    }

    [Test]
    public async Task A_nested_path_under_a_guarded_prefix_is_guarded_on_the_external_port()
    {
        using var host = await CreateHostAsync(InternalPort, "/metrics", "/lattice/scale");

        // StartsWithSegments matches deeper paths under a guarded prefix.
        var nested = await StatusOnPortAsync(host, "/lattice/scale/detail", ExternalPort);

        Assert.That(nested, Is.EqualTo((int)HttpStatusCode.NotFound));
    }

    [Test]
    public async Task An_unguarded_path_is_served_on_the_external_port()
    {
        using var host = await CreateHostAsync(InternalPort, "/metrics", "/lattice/scale");

        // /health and the gRPC facades are not guarded, so they answer on the
        // external port regardless (Front Door's og-state probes /health there).
        var health = await StatusOnPortAsync(host, "/health", ExternalPort);
        var facade = await StatusOnPortAsync(host, "/orleans.lattice.api.state.LatticeState", ExternalPort);

        Assert.Multiple(() =>
        {
            Assert.That(health, Is.EqualTo((int)HttpStatusCode.OK));
            Assert.That(facade, Is.EqualTo((int)HttpStatusCode.OK));
        });
    }

    [Test]
    public async Task A_path_that_only_character_prefix_matches_a_guarded_segment_is_not_guarded()
    {
        using var host = await CreateHostAsync(InternalPort, "/metrics", "/lattice/scale");

        // "/metricsz" shares a character prefix with "/metrics" but is a different
        // segment, so segment-based matching must NOT guard it.
        var status = await StatusOnPortAsync(host, "/metricsz", ExternalPort);

        Assert.That(status, Is.EqualTo((int)HttpStatusCode.OK));
    }

    [Test]
    public async Task No_guarded_prefixes_leaves_the_pipeline_unchanged()
    {
        using var host = await CreateHostAsync(InternalPort);

        var status = await StatusOnPortAsync(host, "/metrics", ExternalPort);

        Assert.That(status, Is.EqualTo((int)HttpStatusCode.OK));
    }

    [Test]
    public async Task A_whitespace_prefix_is_ignored()
    {
        using var host = await CreateHostAsync(InternalPort, "   ");

        var status = await StatusOnPortAsync(host, "/metrics", ExternalPort);

        Assert.That(status, Is.EqualTo((int)HttpStatusCode.OK));
    }
}
