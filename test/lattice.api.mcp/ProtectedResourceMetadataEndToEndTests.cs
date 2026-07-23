using System.Net;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// End-to-end coverage that the opt-in OAuth 2.0 Protected Resource Metadata
/// (RFC 9728) wiring behaves over a real HTTP host: the well-known metadata
/// document is served anonymously, and the scheme-agnostic challenge middleware
/// appends the <c>resource_metadata</c> hint to a <c>401</c> bearer challenge on
/// the MCP transport path only.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives real HTTP
/// requests, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class ProtectedResourceMetadataEndToEndTests
{
    [Test]
    public async Task Well_known_document_is_served_anonymously_with_the_configured_metadata()
    {
        await using var host = await StartHostAsync();
        using var http = new HttpClient { BaseAddress = new Uri(host.Urls.First()) };

        using var response = await http.GetAsync("/.well-known/oauth-protected-resource");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(response.Content.Headers.ContentType?.MediaType, Is.EqualTo("application/json"));

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = doc.RootElement;
        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("resource").GetString(), Is.EqualTo("https://mcp.example.com/"));
            Assert.That(
                root.GetProperty("authorization_servers").EnumerateArray().Select(e => e.GetString()),
                Is.EqualTo(new[] { "https://login.example.com/tenant/v2.0" }));
            Assert.That(
                root.GetProperty("scopes_supported").EnumerateArray().Select(e => e.GetString()),
                Is.EqualTo(new[] { "api://server/.default" }));
        });

        await host.StopAsync();
    }

    [Test]
    public async Task Challenge_hint_is_appended_on_the_transport_path_and_nowhere_else()
    {
        await using var host = await StartHostAsync();
        using var http = new HttpClient { BaseAddress = new Uri(host.Urls.First()) };

        using var onPath = await http.GetAsync("/mcp/probe");
        using var offPath = await http.GetAsync("/elsewhere/probe");

        Assert.Multiple(() =>
        {
            Assert.That(onPath.StatusCode, Is.EqualTo(HttpStatusCode.Unauthorized));
            Assert.That(onPath.Headers.GetValues("WWW-Authenticate").Single(),
                Is.EqualTo("Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\""));

            Assert.That(offPath.StatusCode, Is.EqualTo(HttpStatusCode.Unauthorized));
            Assert.That(offPath.Headers.GetValues("WWW-Authenticate").Single(), Is.EqualTo("Bearer"),
                "The hint must be scoped to the MCP transport path, leaving co-hosted 401s untouched.");
        });

        await host.StopAsync();
    }

    [Test]
    public async Task Well_known_document_bypasses_a_fail_closed_fallback_authorization_policy()
    {
        // A fail-closed host installs a fallback policy requiring an authenticated
        // user. The metadata document must still be reachable anonymously - a
        // client fetches it precisely because it was rejected - while the MCP
        // endpoint stays default-denied.
        await using var host = await StartHostAsync(failClosed: true);
        using var http = new HttpClient { BaseAddress = new Uri(host.Urls.First()) };

        using var metadata = await http.GetAsync("/.well-known/oauth-protected-resource");
        using var transport = await http.GetAsync("/mcp");

        Assert.Multiple(() =>
        {
            Assert.That(metadata.StatusCode, Is.EqualTo(HttpStatusCode.OK),
                "The AllowAnonymous metadata endpoint must bypass the fail-closed fallback policy.");
            Assert.That(transport.StatusCode, Is.EqualTo(HttpStatusCode.Unauthorized),
                "The MCP transport must remain default-denied to anonymous callers.");
            Assert.That(transport.Headers.GetValues("WWW-Authenticate").Single(),
                Does.Contain("resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\""),
                "The real bearer challenge must carry the discovery hint.");
        });

        await host.StopAsync();
    }

    [Test]
    public void MapLatticeMcp_throws_when_metadata_is_opted_in_without_a_resource()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.ProtectedResourceMetadata = new LatticeApiMcpProtectedResourceMetadata();
        });

        var app = builder.Build();

        Assert.That(app.MapLatticeMcp, Throws.InstanceOf<InvalidOperationException>());
    }

    private static async Task<WebApplication> StartHostAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.TransportPattern = "/mcp";
            o.ProtectedResourceMetadata = new LatticeApiMcpProtectedResourceMetadata
            {
                Resource = new Uri("https://mcp.example.com"),
                AuthorizationServers = { new Uri("https://login.example.com/tenant/v2.0") },
                ScopesSupported = { "api://server/.default" },
            };
        });

        var app = builder.Build();
        app.MapLatticeMcp();

        // Two bare 401 bearer challenges: one under the MCP transport path, one
        // outside it, so the scoping of the discovery hint is observable.
        app.MapGet("/mcp/probe", Challenge401);
        app.MapGet("/elsewhere/probe", Challenge401);

        await app.StartAsync();
        return app;

        static Task Challenge401(HttpContext ctx)
        {
            ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
            ctx.Response.Headers.WWWAuthenticate = "Bearer";
            return Task.CompletedTask;
        }
    }

    private static async Task<WebApplication> StartHostAsync(bool failClosed)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        if (failClosed)
        {
            builder.Services.AddAuthentication("Test")
                .AddScheme<AuthenticationSchemeOptions, NoopAuthHandler>("Test", null);
            builder.Services.AddAuthorization(o =>
                o.FallbackPolicy = new AuthorizationPolicyBuilder("Test").RequireAuthenticatedUser().Build());
        }

        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = failClosed;
            o.TransportPattern = "/mcp";
            o.ProtectedResourceMetadata = new LatticeApiMcpProtectedResourceMetadata
            {
                Resource = new Uri("https://mcp.example.com"),
                AuthorizationServers = { new Uri("https://login.example.com/tenant/v2.0") },
                ScopesSupported = { "api://server/.default" },
            };
        });

        var app = builder.Build();
        app.MapLatticeMcp();
        await app.StartAsync();
        return app;
    }

    private sealed class NoopAuthHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
            => Task.FromResult(AuthenticateResult.NoResult());
    }
}
