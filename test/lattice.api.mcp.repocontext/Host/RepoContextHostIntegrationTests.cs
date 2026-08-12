using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Integration tests that stand up the real <see cref="RepoContextHostBuilder"/>
/// wiring - the local durability profile (SQLite grain storage / reminders plus
/// the file WAL on a temp data root) over an in-process test server - and assert
/// the container's #1435 guarantees: restart durability across a full process
/// recreation with a forced WAL replay, the health-probe lifecycle, and the
/// azure-only scaling surface.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo with durable
/// SQLite + file-WAL storage and drives the host end to end, so it is excluded
/// from the fast unit dev loop. The fixture is single-silo and uses the default
/// localhost-clustering ports; the assembly runs fixtures sequentially so the
/// ports never collide.
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class RepoContextHostIntegrationTests
{
    private const string ProbeTree = RepoContextHostTrees.Memory;
    private const string ProbeKey = "durability-probe";

    private string _dataRoot = null!;

    private static CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [SetUp]
    public void SetUp()
        => _dataRoot = Path.Combine(Path.GetTempPath(), "repocontext-it-" + Guid.NewGuid().ToString("N"));

    [TearDown]
    public void TearDown()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_dataRoot))
        {
            try
            {
                Directory.Delete(_dataRoot, recursive: true);
            }
            catch (IOException)
            {
                // A background flush may briefly hold the WAL file; best-effort cleanup.
            }
        }
    }

    private RepoContextHostConfiguration LocalConfig()
        => RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                    [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-it",
                    [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-it",
                })
                .Build());

    private static WebApplication BuildLocalHost(RepoContextHostConfiguration config)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        return RepoContextHostBuilder.Build(builder, config);
    }

    private static async Task WaitForReadyAsync(WebApplication app)
    {
        var readiness = app.Services.GetRequiredService<RepoContextReadinessState>();
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (!readiness.IsReady && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, Ct);
        }

        Assert.That(readiness.IsReady, Is.True, "The host did not reach readiness within the timeout.");
    }

    [Test]
    public async Task Data_written_before_restart_survives_a_full_process_recreation()
    {
        var payload = Encoding.UTF8.GetBytes("remembered-across-restart");

        // First host: write a value through the real durable cluster, then stop
        // gracefully so the WAL commit-log drains to the file on the data root.
        var first = BuildLocalHost(LocalConfig());
        await first.StartAsync(Ct);
        try
        {
            await WaitForReadyAsync(first);
            using (LatticeCredentialContext.Use(LocalTrustedAgent.SubjectId, scheme: LocalTrustedAgent.Scheme))
            {
                var tree = first.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(ProbeTree);
                await tree.SetAsync(ProbeKey, payload, Ct);
            }
        }
        finally
        {
            await first.StopAsync(Ct);
            await first.DisposeAsync();
        }

        // A brand-new host process over the SAME data root: the in-memory
        // projection is gone, so reading the key forces a WAL replay / cold rebuild.
        var second = BuildLocalHost(LocalConfig());
        await second.StartAsync(Ct);
        try
        {
            await WaitForReadyAsync(second);
            using (LatticeCredentialContext.Use(LocalTrustedAgent.SubjectId, scheme: LocalTrustedAgent.Scheme))
            {
                var tree = second.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(ProbeTree);
                var recalled = await tree.GetAsync(ProbeKey, Ct);

                Assert.That(recalled, Is.Not.Null, "The value must survive a full container recreation.");
                Assert.That(recalled, Is.EqualTo(payload));
            }
        }
        finally
        {
            await second.StopAsync(Ct);
            await second.DisposeAsync();
        }
    }

    [Test]
    public async Task Readiness_is_not_ready_until_warmup_then_flips_not_ready_on_drain()
    {
        var app = BuildLocalHost(LocalConfig());
        await app.StartAsync(Ct);
        try
        {
            var client = app.GetTestServer().CreateClient();

            await WaitForReadyAsync(app);

            var live = await client.GetAsync(RepoContextHostBuilder.LivenessPath, Ct);
            var ready = await client.GetAsync(RepoContextHostBuilder.ReadinessPath, Ct);
            Assert.Multiple(() =>
            {
                Assert.That(live.StatusCode, Is.EqualTo(HttpStatusCode.OK), "Liveness is healthy once the process is up.");
                Assert.That(ready.StatusCode, Is.EqualTo(HttpStatusCode.OK), "Readiness is healthy once warmup completes.");
            });

            // Simulate the very start of graceful drain: readiness must flip
            // not-ready before the silo begins to stop.
            app.Services.GetRequiredService<RepoContextReadinessState>().BeginDrain();
            var draining = await client.GetAsync(RepoContextHostBuilder.ReadinessPath, Ct);
            var stillLive = await client.GetAsync(RepoContextHostBuilder.LivenessPath, Ct);
            Assert.Multiple(() =>
            {
                Assert.That(
                    draining.StatusCode,
                    Is.EqualTo(HttpStatusCode.ServiceUnavailable),
                    "Readiness reports not-ready at the start of drain.");
                Assert.That(
                    stillLive.StatusCode,
                    Is.EqualTo(HttpStatusCode.OK),
                    "Liveness stays healthy during drain (the process is still up).");
            });
        }
        finally
        {
            await app.StopAsync(Ct);
            await app.DisposeAsync();
        }
    }

    [Test]
    public void Local_host_opts_past_the_default_deny_mcp_gate()
    {
        // Regression: the container must register a permissive coarse MCP
        // authorizer. Left unregistered, AddLatticeMcp's default-deny
        // DenyAllMcpAuthorizer withholds every repocontext_* tool from
        // tools/list even though lattice_capabilities reports the group
        // available - making the whole surface unreachable. Real enforcement
        // stays on the fail-closed per-tree access gate underneath.
        var app = BuildLocalHost(LocalConfig());
        try
        {
            var authorizer = app.Services.GetRequiredService<ILatticeApiMcpAuthorizer>();
            Assert.That(
                authorizer,
                Is.Not.InstanceOf<DenyAllMcpAuthorizer>(),
                "The container must opt past the default-deny MCP gate; DenyAll hides every repocontext_* tool.");

            var permitted = authorizer.IsAuthorizedAsync(
                new LatticeApiMcpAuthorizationContext(new DefaultHttpContext(), "repocontext_bootstrap"),
                Ct).GetAwaiter().GetResult();
            Assert.That(
                permitted,
                Is.True,
                "The coarse MCP gate must permit the repocontext tools to reach the fail-closed data gate.");
        }
        finally
        {
            app.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    [Test]
    public void Local_profile_does_not_map_the_scaling_endpoint()
    {
        var app = BuildLocalHost(LocalConfig());
        try
        {
            Assert.That(MappedRoutes(app), Does.Not.Contain("/lattice/scale"));
        }
        finally
        {
            app.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    [Test]
    public void Azure_profile_maps_the_scaling_endpoint()
    {
        // A fake connection string satisfies the fail-fast credential check; the
        // host is built (DI + endpoint mapping) but never started, so no Azure
        // connection is attempted.
        var azureConfig = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DurabilityKey] = "azure",
                    [RepoContextHostConfiguration.AzureConnectionKey] = "UseDevelopmentStorage=true",
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        var app = RepoContextHostBuilder.Build(builder, azureConfig);
        try
        {
            Assert.That(MappedRoutes(app), Does.Contain("/lattice/scale"));
        }
        finally
        {
            app.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private static IReadOnlyList<string> MappedRoutes(WebApplication app)
    {
        var routes = new List<string>();
        foreach (var dataSource in ((IEndpointRouteBuilder)app).DataSources)
        {
            foreach (var endpoint in dataSource.Endpoints)
            {
                if (endpoint is RouteEndpoint routeEndpoint)
                {
                    routes.Add("/" + routeEndpoint.RoutePattern.RawText?.TrimStart('/'));
                }
            }
        }

        return routes;
    }
}
