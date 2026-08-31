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
using Microsoft.Extensions.Options;
using ModelContextProtocol.AspNetCore;
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

    /// <summary>
    /// Waits for the host's <b>actual</b> readiness contract by polling
    /// <c>/health/ready</c> until it returns 200, exactly as an orchestrator does.
    /// <para>
    /// It deliberately does not wait on any single readiness component. The endpoint
    /// is the conjunction of the lifecycle-phase check and the vector-plane retrieval
    /// check, and the lifecycle half flips as soon as the access grant is seeded -
    /// well before the retrieval warmup has proven the plane can serve. Waiting on one
    /// component would therefore be a weaker precondition than this helper's name
    /// claims, and would silently drift again the next time the composition changes.
    /// </para>
    /// </summary>
    private static async Task WaitForReadyAsync(WebApplication app)
    {
        using var client = app.GetTestServer().CreateClient();
        var deadline = DateTime.UtcNow.AddSeconds(60);

        HttpStatusCode status;
        do
        {
            status = (await client.GetAsync(RepoContextHostBuilder.ReadinessPath, Ct)).StatusCode;
            if (status == HttpStatusCode.OK)
            {
                return;
            }

            await Task.Delay(100, Ct);
        }
        while (DateTime.UtcNow < deadline);

        Assert.Fail(
            $"The host did not report ready within the timeout: {RepoContextHostBuilder.ReadinessPath} last returned {status}.");
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
    public async Task Indexing_run_writes_under_the_local_agent_even_with_no_ambient_credential()
    {
        // Regression for the reminder-driven resume path. A resume after restart
        // re-enqueues the job from ReceiveReminder - a system-origin grain call that
        // carries NO ambient credential - so the background run must stamp the fixed
        // local-agent credential itself, or its structural writes fail closed as
        // 'anonymous' against the default-deny gate. This test drives the runner
        // with no ambient credential (exactly the resume condition) and asserts the
        // structural writes land and the job completes.
        var repoRoot = Path.Combine(_dataRoot, "sample-repo");
        Directory.CreateDirectory(repoRoot);
        await File.WriteAllTextAsync(
            Path.Combine(repoRoot, "alpha.cs"), "namespace Sample; public class Alpha { }", Ct);
        await File.WriteAllTextAsync(
            Path.Combine(repoRoot, "beta.md"), "# Beta\nSome text.", Ct);

        // The workspace guard is re-checked inside the run, so the sample repo must
        // resolve under the configured workspace root.
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _dataRoot,
                    [RepoContextHostConfiguration.ClusterIdKey] = "repocontext-it",
                    [RepoContextHostConfiguration.ServiceIdKey] = "repocontext-it",
                    [RepoContextHostConfiguration.WorkspaceRootKey] = _dataRoot,
                })
                .Build());

        var app = BuildLocalHost(config);
        await app.StartAsync(Ct);
        try
        {
            await WaitForReadyAsync(app);
            var grainFactory = app.Services.GetRequiredService<IGrainFactory>();

            // Negative control: with no ambient credential, a direct structural
            // write is anonymous and the default-deny gate denies it. This proves
            // the gate is live, so the positive case below is a real grant - not an
            // accident of a disabled gate.
            Assert.That(
                async () =>
                {
                    var tree = grainFactory.GetGrain<ILattice>(RepoContextHostTrees.Structural);
                    await tree.SetAsync("regression-probe", Encoding.UTF8.GetBytes("x"), Ct);
                },
                Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
                "An un-stamped anonymous structural write must be denied by the default-deny gate.");

            // Positive: drive the runner with no ambient credential (the resume
            // condition). The runner must stamp the local-agent credential so the
            // structural writes succeed and the job completes.
            var runner = app.Services.GetRequiredService<IRepoIndexRunner>();
            var request = new RepoIndexJobRequest
            {
                RepoRoot = repoRoot,
                RepoId = "resume-auth-it",
            };

            Assert.That(
                LatticeCredentialContext.IsActive,
                Is.False,
                "The run is started with no ambient credential, mirroring the reminder-driven resume.");

            await runner.StartIndexAsync(request);

            var progress = await PollUntilTerminalAsync(runner, "resume-auth-it");

            Assert.Multiple(() =>
            {
                Assert.That(
                    progress.Status,
                    Is.EqualTo(RepoIndexStatus.Completed),
                    $"The index must complete under the stamped local-agent credential (error: {progress.Error}).");
                Assert.That(
                    progress.Error,
                    Is.Null,
                    "A completed run under the stamped credential records no authorization error.");
                Assert.That(
                    progress.FilesAdded,
                    Is.GreaterThan(0),
                    "The structural writes must have landed the walked files under the stamped credential.");
            });
        }
        finally
        {
            await app.StopAsync(Ct);
            await app.DisposeAsync();
        }
    }

    private static async Task<RepoIndexProgress> PollUntilTerminalAsync(
        IRepoIndexRunner runner, string repoId)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            var progress = await runner.GetProgressAsync(repoId);
            if (progress.Status is RepoIndexStatus.Completed or RepoIndexStatus.Failed)
            {
                return progress;
            }

            await Task.Delay(100, Ct);
        }

        Assert.Fail("The indexing job did not reach a terminal state within the timeout.");
        throw new InvalidOperationException("unreachable");
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

            // Pin WHICH arm of the retrieval readiness machine makes this
            // configuration ready, so a regression that leaves the box permanently
            // 503 fails here with a clear cause rather than as a mute timeout.
            // This fixture always binds an embedding provider (the host registers the
            // Onyx client unconditionally), so the keyword-only arm cannot fire; it is
            // the "no repositories indexed, so there is nothing the plane could fail to
            // serve" arm that reaches Serving.
            var retrieval = app.Services.GetRequiredService<RepoContextRetrievalReadinessState>();
            Assert.Multiple(() =>
            {
                Assert.That(
                    app.Services.GetService<IEmbeddingProvider>(),
                    Is.Not.Null,
                    "The host binds an embedding provider unconditionally, so the keyword-only readiness arm is not what makes this configuration ready.");
                Assert.That(
                    retrieval.Phase,
                    Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
                    "An indexed-nothing host reaches retrieval readiness through the Serving arm; Building here would mean the endpoint never returns 200.");
                Assert.That(
                    retrieval.TimeToReady,
                    Is.Not.Null,
                    "Reaching a ready phase must stamp the time-to-retrieval-ready figure the cold-start work is measured by.");
            });

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
    public void Local_host_runs_the_mcp_transport_stateless()
    {
        // Regression: the container exposes a fixed repocontext_* tool set to one
        // trusted local agent (RequireAuthorization off, AllowAll authorizer), so it
        // needs none of the permission-scoped per-session tool collections that
        // stateful mode serves. It must run the streamable-HTTP transport STATELESS
        // so a container restart or recreation does not expire the client's in-memory
        // session and 404 every subsequent tool call until the client reconnects.
        var app = BuildLocalHost(LocalConfig());
        try
        {
            var transport = app.Services.GetRequiredService<IOptions<HttpServerTransportOptions>>().Value;
            Assert.That(
                transport.Stateless,
                Is.True,
                "The container's MCP transport must be stateless so a restart does not expire the client session.");
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
