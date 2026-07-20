using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Client;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// End-to-end coverage that the default-deny coarse authorizer
/// (<see cref="DenyAllMcpAuthorizer"/>, registered by
/// <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>) is actually
/// enforced on a real MCP session served over HTTP - not dead code. A real
/// <see cref="McpClient"/> connects to an in-process Kestrel host that has the
/// data tool module registered and a stub credential bridge / permission
/// resolver that grant the caller the data group, so the only thing that can
/// keep the data tools out of reach is the authorizer. The test proves the
/// authorizer both hides the data tools from discovery and rejects a direct
/// invocation.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives the full
/// MCP streamable-HTTP handshake, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class DenyAllMcpAuthorizerEndToEndTests
{
    [Test]
    public async Task Deny_all_authorizer_blocks_data_tools_end_to_end()
    {
        await using var host = await StartHostAsync();
        var endpoint = new Uri(host.Urls.First(), UriKind.Absolute);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var transport = new HttpClientTransport(
            new HttpClientTransportOptions
            {
                Endpoint = endpoint,
                TransportMode = HttpTransportMode.StreamableHttp,
            });
        await using var client = await McpClient.CreateAsync(transport, cancellationToken: cts.Token);

        var tools = await client.ListToolsAsync(cancellationToken: cts.Token);
        var toolNames = tools.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(toolNames, Does.Contain("lattice_capabilities"),
                "The authenticated session still receives the ungated capabilities meta-tool.");
            Assert.That(toolNames, Does.Not.Contain("lattice_data_get"),
                "The default-deny authorizer must hide the granted data tools from discovery.");
        });

        // A direct invocation of a data tool is rejected: whether it is unlisted
        // or gated at invoke time, the default-deny posture blocks it end to end.
        Assert.ThrowsAsync<McpException>(
            () => client.CallToolAsync(
                "lattice_data_get",
                new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k" },
                cancellationToken: cts.Token).AsTask());

        await host.StopAsync(cts.Token);
    }

    private static async Task<WebApplication> StartHostAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        // Stub the identity and permission collaborators so, absent the
        // authorizer, the data tools WOULD be advertised to this caller. This
        // isolates the default-deny authorizer as the sole reason they are not.
        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new StubBridge(new LatticeCredential("agent")));
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new StubResolver(LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data)));

        // RequireAuthorization=false only relaxes the ASP.NET endpoint auth
        // requirement so the loopback client can reach the transport; the
        // default-deny ILatticeApiMcpAuthorizer is left in place and is what the
        // gate enforces.
        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.EnableDataTools = true;
        });
        builder.Services.AddDataTools();

        var app = builder.Build();
        app.MapLatticeMcp();
        await app.StartAsync();
        return app;
    }

    private sealed class StubBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(Microsoft.AspNetCore.Http.HttpContext context) => credential;
    }

    private sealed class StubResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken)
            => new(access);
    }
}
