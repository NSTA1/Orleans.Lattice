using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// End-to-end regression coverage for the MCP DNS-rebinding guard: with
/// <see cref="LatticeApiMcpOptions.RequireAllowedOrigin"/> on, a browser request
/// to the MCP transport carrying a foreign <c>Origin</c> is refused with
/// <c>403</c> before any handler runs, while non-browser clients (no
/// <c>Origin</c>), allow-listed origins, and loopback origins pass.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives real HTTP
/// requests, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class McpOriginValidationEndToEndTests
{
    [Test]
    public async Task Foreign_origin_is_refused_on_the_transport_path()
    {
        await using var host = await StartHostAsync(o => o.RequireAllowedOrigin = true);

        using var response = await SendAsync(host, "/mcp/probe", "http://attacker.example");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task Foreign_origin_never_reaches_the_mcp_handler()
    {
        await using var host = await StartHostAsync(o => o.RequireAllowedOrigin = true);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
        {
            Content = new StringContent(
                """{"jsonrpc":"2.0","id":1,"method":"tools/list"}""",
                System.Text.Encoding.UTF8,
                "application/json"),
        };
        request.Headers.Add("Origin", "http://rebound.attacker.example:8080");
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        using var http = new HttpClient { BaseAddress = new Uri(host.Urls.First()) };

        using var response = await http.SendAsync(request);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task Request_without_an_origin_passes()
    {
        await using var host = await StartHostAsync(o => o.RequireAllowedOrigin = true);

        using var response = await SendAsync(host, "/mcp/probe", origin: null);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task Allow_listed_origin_passes_case_insensitively()
    {
        await using var host = await StartHostAsync(o =>
        {
            o.RequireAllowedOrigin = true;
            o.AllowLoopbackOrigins = false;
            o.AllowedOrigins.Add("https://Inspector.Example.com/");
        });

        using var allowed = await SendAsync(host, "/mcp/probe", "https://inspector.example.com");
        using var otherPort = await SendAsync(host, "/mcp/probe", "https://inspector.example.com:8443");

        Assert.Multiple(() =>
        {
            Assert.That(allowed.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(otherPort.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden),
                "An allow-list entry matches the exact origin, port included.");
        });
    }

    [Test]
    public async Task Loopback_origin_passes_by_default_and_is_refused_when_disallowed()
    {
        await using var loopbackOn = await StartHostAsync(o => o.RequireAllowedOrigin = true);
        await using var loopbackOff = await StartHostAsync(o =>
        {
            o.RequireAllowedOrigin = true;
            o.AllowLoopbackOrigins = false;
        });

        using var localhostOn = await SendAsync(loopbackOn, "/mcp/probe", "http://localhost:6274");
        using var ipv4On = await SendAsync(loopbackOn, "/mcp/probe", "http://127.0.0.1:3000");
        using var ipv6On = await SendAsync(loopbackOn, "/mcp/probe", "http://[::1]:3000");
        using var localhostOff = await SendAsync(loopbackOff, "/mcp/probe", "http://localhost:6274");

        Assert.Multiple(() =>
        {
            Assert.That(localhostOn.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(ipv4On.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(ipv6On.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(localhostOff.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
        });
    }

    [Test]
    public async Task Opaque_null_origin_is_refused()
    {
        await using var host = await StartHostAsync(o => o.RequireAllowedOrigin = true);

        using var response = await SendAsync(host, "/mcp/probe", "null");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.Forbidden));
    }

    [Test]
    public async Task Paths_outside_the_transport_are_untouched()
    {
        await using var host = await StartHostAsync(o => o.RequireAllowedOrigin = true);

        using var response = await SendAsync(host, "/elsewhere/probe", "http://attacker.example");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    [Test]
    public async Task Validation_is_off_by_default()
    {
        await using var host = await StartHostAsync(_ => { });

        using var response = await SendAsync(host, "/mcp/probe", "http://attacker.example");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    }

    private static async Task<HttpResponseMessage> SendAsync(WebApplication host, string path, string? origin)
    {
        using var http = new HttpClient { BaseAddress = new Uri(host.Urls.First()) };
        using var request = new HttpRequestMessage(HttpMethod.Post, path);
        if (origin is not null)
        {
            request.Headers.Add("Origin", origin);
        }

        return await http.SendAsync(request);
    }

    private static async Task<WebApplication> StartHostAsync(Action<LatticeApiMcpOptions> configure)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.TransportPattern = "/mcp";
            configure(o);
        });

        var app = builder.Build();
        app.MapLatticeMcp();
        app.MapPost("/mcp/probe", () => "ok");
        app.MapPost("/elsewhere/probe", () => "ok");

        await app.StartAsync();
        return app;
    }
}
