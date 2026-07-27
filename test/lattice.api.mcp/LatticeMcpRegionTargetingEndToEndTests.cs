using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// End-to-end coverage of region targeting (issue #1364) driven through a real
/// <see cref="McpClient"/> against an in-process Kestrel host wired with two
/// regions. Proves the whole seam over the wire: the <c>lattice_list_regions</c>
/// discovery tool reports both configured regions current-first, an explicit
/// <c>region</c> selector on an ordinary tool call returns the result annotated
/// with the served region, omitting the selector leaves the result unannotated
/// (byte-for-byte unchanged), and targeting an unknown region yields a clean typed
/// fault rather than a leaked exception.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives the full MCP
/// streamable-HTTP handshake, so it is excluded from the fast unit dev loop. The
/// host uses the in-silo binding with a manually registered two-region router, so
/// the routing resolution, ambient scope, and served-region annotation are all
/// exercised end-to-end; the per-region gRPC channel selection is covered by the
/// <see cref="LatticeMcpRegionRoutingWiringTests"/> unit tests.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeMcpRegionTargetingEndToEndTests
{
    [Test]
    public async Task List_regions_reports_both_configured_regions_current_first()
    {
        await using var host = await StartHostAsync();
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_list_regions",
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var json = result.StructuredContent!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(json.GetProperty("currentRegion").GetString(), Is.EqualTo("us"));
            var regions = json.GetProperty("regions");
            Assert.That(regions.GetArrayLength(), Is.EqualTo(2));
            Assert.That(regions[0].GetProperty("regionId").GetString(), Is.EqualTo("us"));
            Assert.That(regions[0].GetProperty("isCurrent").GetBoolean(), Is.True);
            Assert.That(regions[1].GetProperty("regionId").GetString(), Is.EqualTo("eu"));
        });
    }

    [Test]
    public async Task Explicit_region_returns_the_result_annotated_with_the_served_region()
    {
        await using var host = await StartHostAsync();
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k", ["region"] = "eu" },
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(result.Meta, Is.Not.Null, "An explicitly targeted call must be annotated.");
            Assert.That(result.Meta!["region"]!.GetValue<string>(), Is.EqualTo("eu"));
        });
    }

    [Test]
    public async Task Omitting_region_leaves_the_result_unannotated()
    {
        await using var host = await StartHostAsync();
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k" },
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            var hasRegion = result.Meta is not null && result.Meta.ContainsKey("region");
            Assert.That(hasRegion, Is.False,
                "The default-region path must be byte-for-byte unchanged and carry no region annotation.");
        });
    }

    [Test]
    public async Task Unknown_region_yields_a_clean_typed_fault()
    {
        await using var host = await StartHostAsync();
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k", ["region"] = "mars" },
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var text = result.Content.OfType<TextContentBlock>().FirstOrDefault()?.Text ?? string.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(text, Does.Contain("Unknown region 'mars'"));
            Assert.That(text, Does.Contain("lattice_list_regions"),
                "The fault must point the caller at discovery, not leak an exception.");
        });
    }

    [Test]
    public async Task Verified_peer_region_serves_the_targeted_call()
    {
        await using var host = await StartVerifyingHostAsync(stateClusterId: "cluster-eu");
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k", ["region"] = "eu" },
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(result.Meta!["region"]!.GetValue<string>(), Is.EqualTo("eu"));
        });
    }

    [Test]
    public async Task Peer_region_that_fails_identity_verification_is_rejected_fail_closed()
    {
        // The peer's endpoint answers as a different cluster - the anycast/Front-Door
        // "wrong region" trap. The gate must refuse it rather than silently serve it.
        await using var host = await StartVerifyingHostAsync(stateClusterId: "cluster-wrong");
        await using var client = await ConnectAsync(host);

        var result = await client.CallToolAsync(
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k", ["region"] = "eu" },
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var text = result.Content.OfType<TextContentBlock>().FirstOrDefault()?.Text ?? string.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(text, Does.Contain("failed identity verification"));
            Assert.That(text, Does.Contain("Front Door"),
                "The fault must name the likely cause so an operator can correct the endpoint.");
        });
    }

    private static async Task<McpClient> ConnectAsync(WebApplication host)
    {
        var transport = new HttpClientTransport(
            new HttpClientTransportOptions
            {
                Endpoint = new Uri(host.Urls.First(), UriKind.Absolute),
                TransportMode = HttpTransportMode.StreamableHttp,
            });
        return await McpClient.CreateAsync(
            transport, cancellationToken: TestContext.CurrentContext.CancellationToken);
    }

    private static async Task<WebApplication> StartHostAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new StubBridge(new LatticeCredential("agent")));
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new StubResolver(LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data)));
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());
        builder.Services.AddSingleton<ILatticeDataApi>(new FoundDataApi());

        // Register a two-region router before AddLatticeMcp so its TryAdd no-ops
        // and the catalog is projected over this topology. Both regions serve the
        // data group; the peer is reachable so it is listed (fail-closed discovery
        // still holds - an unconfigured group would be omitted).
        builder.Services.AddSingleton<ILatticeApiMcpRegionRouter>(
            new LatticeApiMcpRegionRouter("us", new[]
            {
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = "us",
                    ClusterId = "cluster-us",
                    IsCurrent = true,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.Data] = null },
                },
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = "eu",
                    ClusterId = "cluster-eu",
                    IsCurrent = false,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?>
                    {
                        [LatticeApiMcpGroup.Data] = "https://eu-data:5001",
                    },
                },
            }));

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

    private static async Task<WebApplication> StartVerifyingHostAsync(string stateClusterId)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new StubBridge(new LatticeCredential("agent")));
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new StubResolver(LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data)));
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());
        builder.Services.AddSingleton<ILatticeDataApi>(new FoundDataApi());

        // The EU peer serves both data (so the call can be routed there) and state
        // (so the verifier has a facade to probe). It advertises cluster-eu; the
        // probe answers as stateClusterId, so the caller chooses match or mismatch.
        builder.Services.AddSingleton<ILatticeApiMcpRegionRouter>(
            new LatticeApiMcpRegionRouter("us", new[]
            {
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = "us",
                    ClusterId = "cluster-us",
                    IsCurrent = true,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.Data] = null },
                },
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = "eu",
                    ClusterId = "cluster-eu",
                    IsCurrent = false,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?>
                    {
                        [LatticeApiMcpGroup.Data] = "https://eu-data:5001",
                        [LatticeApiMcpGroup.State] = "https://eu-state:5001",
                    },
                },
            }));

        var stateQuery = Substitute.For<ILatticeStateQuery>();
        stateQuery.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = stateClusterId, ServiceId = "svc" });
        builder.Services.AddSingleton(stateQuery);
        builder.Services.AddSingleton<ILatticeApiMcpRegionIdentityVerifier>(
            static sp => new LatticeApiMcpRegionIdentityVerifier(
                sp.GetRequiredService<ILatticeApiMcpRegionRouter>(), sp));

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

    private sealed class FoundDataApi : ILatticeDataApi
    {
        public Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => Task.FromResult(new DataReadResult
            {
                TreeId = treeId,
                Key = key,
                Found = true,
                Value = new byte[] { 1 },
            });

        public Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task SetManyAtomicAsync(string treeId, DataAtomicBatch batch, string operationId, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(IReadOnlyList<DataTreeBatch> batches, string operationId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task SetManyAsync(string treeId, IReadOnlyList<DataEntry> upserts, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    }
}
