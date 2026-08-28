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
/// authorizer hides the data tools from discovery and that a denied data tool
/// is unreachable end to end.
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
            Assert.That(toolNames, Does.Not.Contain("lattice_list_regions"),
                "lattice_capabilities is the only ungated advertisement: the region-discovery tool "
                + "discloses peer-region ids, cluster ids, and per-group endpoints, so the default-deny "
                + "authorizer must hide it too.");
        });

        // The region-discovery tool is equally unreachable by name: it is gated in
        // lock-step, so a client that asks for it directly is refused rather than
        // served cluster topology.
        Assert.That(
            () => client.CallToolAsync(
                "lattice_list_regions",
                cancellationToken: cts.Token).AsTask(),
            Throws.InstanceOf<McpException>());

        // A direct invocation of a denied data tool is unreachable end to end.
        // Because the gate is lock-step, DenyAll hides lattice_data_get at
        // discovery, so it is never registered in the session and a direct call
        // fails at the protocol layer (McpProtocolException, derived from
        // McpException) before it can reach the CredentialStampingTool invoke
        // gate. Assert on the McpException base so the test accepts either the
        // discovery-gate ("unknown tool") or the invoke-gate outcome; the exact
        // invoke-time McpException path is covered by the gate unit tests.
        Assert.That(
            () => client.CallToolAsync(
                "lattice_data_get",
                new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k" },
                cancellationToken: cts.Token).AsTask(),
            Throws.InstanceOf<McpException>());

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
