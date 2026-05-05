using Azure.Data.Tables;
using Azure.Storage.Queues;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Components;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Grpc;
using MultiSiteManufacturing.Host.Inventory;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Host.Operator;
using MultiSiteManufacturing.Host.Replication;

// HTTP/2 cleartext (h2c) for the gRPC push transport. The dev
// docker-compose topology and the local `dotnet run` topology both
// expose plaintext HTTP/2 on the silo HTTP ports; the grpc-dotnet
// channel needs this AppContext switch enabled before constructing a
// GrpcChannel against an http:// URI. Production deployments swap to
// https:// + mTLS (configured via GrpcPushTransportOptions.ConfigureChannel)
// and do not need this switch.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = WebApplication.CreateBuilder(args);

// Cluster-aware bootstrap. The sample launches as one of two
// independent Orleans clusters — "us" and "eu" — selected via
// the --cluster command-line argument. Each cluster has its own ClusterId,
// its own Azurite instance, and its own HTTP port range. A per-cluster
// overlay file (appsettings.cluster.{name}.json) supplies connection
// strings, ports, and the replication topology; it's merged on top of
// appsettings.json so shared defaults still apply.
var clusterName = ResolveArg(args, "--cluster", builder.Configuration["CLUSTER_NAME"]) ?? "us";
builder.Configuration.AddJsonFile($"appsettings.cluster.{clusterName}.json", optional: false, reloadOnChange: false);

// The default WebApplicationBuilder config chain is:
//   appsettings.json -> appsettings.{Env}.json -> env vars -> cmd line
// Later sources win. By appending the cluster overlay above we put JSON
// *after* env vars, which silently invalidates Docker Compose overrides
// like ConnectionStrings__AzureTableStorage. Re-adding env vars + args
// here restores the documented precedence (env/args beat the overlay).
builder.Configuration.AddEnvironmentVariables();
builder.Configuration.AddCommandLine(args);

var useInMemoryStorage = builder.Environment.IsEnvironment("Testing")
    || builder.Configuration.GetValue<bool>("Orleans:UseInMemoryStorage");

var tableStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureTableStorage")
    ?? "UseDevelopmentStorage=true";

// Queue storage for the dashboard broadcast stream. Falls back to the
// same account as table storage (Azurite exposes both services on one
// connection string); override via the ConnectionStrings:AzureQueueStorage
// setting if the deployment puts queues on a different account.
var queueStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureQueueStorage")
    ?? tableStorageConnectionString;

// Cluster section — per-cluster ports, cluster id, etc.
var clusterSection = builder.Configuration.GetSection("Cluster");
var orleansClusterId = clusterSection["OrleansClusterId"] ?? $"msmfg-{clusterName}";

var siloId = ResolveArg(args, "--silo-id", builder.Configuration["SILO_ID"]) ?? "a";
var isPrimarySilo = string.Equals(siloId, "a", StringComparison.OrdinalIgnoreCase);

var siloPort = isPrimarySilo
    ? clusterSection.GetValue("SiloPortA", 11111)
    : clusterSection.GetValue("SiloPortB", 11112);
var gatewayPort = isPrimarySilo
    ? clusterSection.GetValue("GatewayPortA", 30000)
    : clusterSection.GetValue("GatewayPortB", 30001);
var httpPort = isPrimarySilo
    ? clusterSection.GetValue("HttpPortA", 5001)
    : clusterSection.GetValue("HttpPortB", 5002);

// When running under Docker Compose, ASPNETCORE_URLS is set by the
// container env (typically "http://+:8080") and must be honoured as-is —
// binding to "localhost" inside a container only binds the loopback
// interface and the host-side port publish (5001..5004) never reaches
// the app. For the host-process path (plain `dotnet run`),
// ASPNETCORE_URLS is unset and we fall back to the per-A/B httpPort
// from the cluster overlay.
if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ASPNETCORE_URLS")))
{
    builder.WebHost.UseUrls($"http://localhost:{httpPort}");
}

// When the host is launched via `dotnet <dll>` (as run.ps1 does, to avoid
// the shared-bin/obj race of two concurrent `dotnet run` invocations), the
// default environment is Production and ASP.NET Core does not auto-wire the
// Static Web Assets manifest. Without this call, MapStaticAssets serves
// _framework/blazor.web.js as a 200 with 0 bytes — the Blazor bootstrap
// script loads but does nothing, so the interactive SignalR circuit never
// forms and all @onclick handlers (Chaos flyout, Race, Fix) are dead.
builder.WebHost.UseStaticWebAssets();

builder.Services.AddSingleton(new SiloIdentity(siloId, isPrimarySilo, clusterName));

// Load the replication topology once and publish as a singleton
// so the outgoing filter, the log writer, the inbound endpoint, and
// the replicator grain all share one immutable view.
var replicationTopology = ReplicationTopology.Load(builder.Configuration);
builder.Services.AddSingleton(replicationTopology);
builder.Services.AddSingleton<ReplicationLogWriter>();
builder.Services.AddSingleton<LatticeReplicationFilter>();
builder.Services.AddSingleton<ReplicationActivityTracker>();
builder.Services.AddHttpClient<ReplicationHttpClient>();

if (!useInMemoryStorage && replicationTopology.IsEnabled)
{
    // Only run the bootstrap service when replication is enabled and
    // persistent storage is available — in-memory test silos don't
    // have Azure Table reminders, and the janitor + replicator rely
    // on IRemindable.
    builder.Services.AddHostedService<ReplicationBootstrapHostedService>();
}

// Migration step 1: package-shipped replication wired in alongside the
// host-rolled pipeline. The package observes only trees declared in
// LatticeReplicationOptions.ReplicatedTrees - currently `mfg-facts-v2`
// only - so the host-rolled and package pipelines are disjoint by
// tree id and can run side by side. See `samples/MultiSiteManufacturing/migration.md`
// for the full staged plan.
var packagePeerClusterId = builder.Configuration["PackageReplication:PeerClusterId"];
var packagePeerGrpcEndpoint = builder.Configuration["PackageReplication:PeerGrpcEndpoint"];
var packageReplicationConfigured = !useInMemoryStorage
    && !string.IsNullOrWhiteSpace(packagePeerClusterId)
    && !string.IsNullOrWhiteSpace(packagePeerGrpcEndpoint)
    && Uri.TryCreate(packagePeerGrpcEndpoint, UriKind.Absolute, out _);

builder.Host.UseOrleans(silo =>
{
    // Register the outgoing grain-call filter so every ILattice
    // SetAsync/DeleteAsync invocation flows through the filter and is
    // appended to the replog for opted-in trees.
    silo.AddOutgoingGrainCallFilter<LatticeReplicationFilter>();

    if (useInMemoryStorage)
    {
        // Single-silo in-memory mode (tests + quick-start without Azurite).
        silo.UseLocalhostClustering(siloPort, gatewayPort);
        silo.UseInMemoryReminderService();
        silo.AddMemoryGrainStorageAsDefault();
        silo.AddMemoryGrainStorage("msmfgGrainState");
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // Dashboard broadcast stream — in-memory variant. This path is
        // only used by the single-silo quick-start so cluster-wide
        // fan-out is a no-op; the memory provider keeps the broadcaster
        // wiring identical to the production code path.
        silo.AddMemoryStreams(DashboardBroadcaster.StreamProviderName);
        silo.AddMemoryGrainStorage("PubSubStore");
    }
    else
    {
        silo.Configure<ClusterOptions>(o =>
        {
            o.ClusterId = orleansClusterId;
            o.ServiceId = "msmfg-service";
        });

        silo.ConfigureEndpoints(siloPort, gatewayPort, listenOnAnyHostAddress: true);

        silo.UseAzureStorageClustering(o =>
        {
            o.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.UseAzureTableReminderService(o =>
        {
            o.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddAzureTableGrainStorageAsDefault(options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddAzureTableGrainStorage("msmfgGrainState", options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddLattice((services, name) =>
        {
            services.AddAzureTableGrainStorage(name, options =>
            {
                options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
            });
        });

        // Cluster-wide dashboard broadcast stream backed by Azure
        // Storage Queues. Every silo's DashboardBroadcaster publishes
        // each inbound fact onto this stream and subscribes to it, so
        // every Blazor Server circuit — regardless of which silo hosts
        // it — receives every fact. Durable transport survives silo
        // restarts and transient outages: queued messages are picked
        // up when subscribers reconnect. PubSubStore (Azure Table)
        // backs subscription metadata so subscriptions survive restart
        // too.
        silo.AddAzureQueueStreams(
            DashboardBroadcaster.StreamProviderName,
            configurator =>
            {
                configurator.ConfigureAzureQueue(ob => ob.Configure(options =>
                {
                    options.QueueServiceClient = new QueueServiceClient(queueStorageConnectionString);
                    // A small queue count keeps ordering predictable
                    // for a modest-volume dashboard feed. Orleans hashes
                    // the StreamId across these queues; single-queue
                    // gives strict FIFO which is what the dashboard
                    // wants (no reorder relative to publish).
                    options.QueueNames = new List<string> { "msmfgdashboard-0" };
                }));
            });
        silo.AddAzureTableGrainStorage("PubSubStore", options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        // Migration step 1: package-shipped replication. Disjoint from
        // the host-rolled pipeline by tree id - the package's
        // ReplicatedTrees map opts in `mfg-facts-v2` only, while the
        // host-rolled ReplicationTopology covers `mfg-facts`,
        // `mfg-site-activity-index`, `mfg-part-crdt`. Wired only on
        // the persistent-storage path because the package's WAL,
        // shipper, and maintenance grain require Azure Table
        // reminders + grain storage; the in-memory path runs without
        // package replication and is unaffected.
        if (packageReplicationConfigured)
        {
            silo.AddLatticeReplication(opts =>
            {
                opts.ClusterId = clusterName;
                opts.ReplicatedTrees = new Dictionary<string, ReplicationMode>(StringComparer.Ordinal)
                {
                    [PackageReplicationFactMirror.MirrorTreeId] = ReplicationMode.LwwRegister,
                };
                opts.ReplicationPeers = new[] { packagePeerClusterId! };
            });
        }
    }
});

builder.Services.AddGrpc();

// Migration step 1: package-shipped replication's gRPC push transport
// and receiver-side gRPC service. The receiver service piggy-backs on
// the same Kestrel + AddGrpc as the existing FactIngress / SiteControl
// / Compliance / Inventory services; the sender's GrpcChannel is
// constructed lazily on the first ship attempt and reused thereafter
// (HTTP/2 multiplexing). Wired only when a peer endpoint has been
// resolved from configuration so the in-memory test path and partial
// local runs do not trip the transport's strict "every peer must be
// in PeerEndpoints" check.
if (packageReplicationConfigured)
{
    var peerUri = new Uri(packagePeerGrpcEndpoint!, UriKind.Absolute);

    builder.Services.AddLatticeReplicationGrpcServer();
    builder.Services.AddLatticeReplicationGrpcPushTransport(opts =>
    {
        opts.PeerEndpoints[packagePeerClusterId!] = peerUri;
    });

    // Mirror every fact that flows through FederationRouter into the
    // lattice tree the package replicates (`mfg-facts-v2`). Removed at
    // step 2 of the migration when `mfg-facts` itself moves under
    // package replication.
    builder.Services.AddHostedService<PackageReplicationFactMirror>();
}


// OpenTelemetry: export the orleans.lattice and orleans.lattice.replication
// meters via Prometheus. Each silo serves /metrics on its ASP.NET Core port
// (8080 inside the container); the docker-compose `prometheus` service
// scrapes all four silos and the `grafana` service renders the dashboards
// shipped by Orleans.Lattice.Dashboards.
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddPrometheusExporter());

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Federation: one concrete backend per name, each wrapped in a
// ChaosFactBackend decorator so IBackendChaosGrain can inject jitter,
// transient failures, and write amplification.
builder.Services.AddSingleton<BaselineFactBackend>();
builder.Services.AddSingleton<LatticeFactBackend>();
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<BaselineFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<LatticeFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<FederationRouter>();

// Operator action surface.
builder.Services.AddSingleton<OperatorClock>();
builder.Services.AddScoped<OperatorActions>();

// CRDT-typed grain state backed by Orleans.Lattice. Singleton
// because it wraps a single ILattice grain reference and holds no
// per-call state.
builder.Services.AddSingleton<PartCrdtStore>();

// Drains this silo's CRDT shadow prefix back into the shared
// prefix when the simulated inter-silo partition heals.
builder.Services.AddHostedService<PartitionHealHostedService>();

// Keeps a lightweight B+ tree index of {site}/{stage}/{serial}
// entries so the "Parts by site" page can render a live per-site
// inventory via a range scan on ILattice. The index subscribes to
// FederationRouter.FactRouted and writes one entry per Fact — every
// fact type carries a Site, so inspection-only sites (Stuttgart CMM
// Lab) appear alongside process-step sites.
builder.Services.AddSingleton<SiteActivityIndex>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SiteActivityIndex>());

builder.Services.AddSingleton<DashboardBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<DashboardBroadcaster>());

// Seeder runs on exactly one silo of exactly one cluster so the
// two clusters don't race and produce duplicate keys. Suppressed in
// the Testing environment so contract tests start against empty state.
//
// Gating precedence:
//   1. Configuration key "Seeder:Enabled" (env var `Seeder__Enabled`)
//      wins outright when present — `true` forces seeding on this
//      silo, `false` forces it off. Useful in docker-compose / CI
//      where the topology is declarative.
//   2. Otherwise, the default heuristic runs the seeder only on the
//      primary silo of the "us" cluster — the convention the
//      sample's docker-compose and run.ps1 topologies rely on.
var isSeeder = false;
if (!builder.Environment.IsEnvironment("Testing"))
{
    var seederOverride = builder.Configuration["Seeder:Enabled"];
    if (bool.TryParse(seederOverride, out var explicitFlag))
    {
        isSeeder = explicitFlag;
    }
    else
    {
        isSeeder = isPrimarySilo
            && string.Equals(clusterName, "us", StringComparison.OrdinalIgnoreCase);
    }
}
if (isSeeder)
{
    builder.Services.AddHostedService<InventorySeeder>();
}

var app = builder.Build();

app.MapStaticAssets();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<FactIngressServiceImpl>();
app.MapGrpcService<SiteControlServiceImpl>();
app.MapGrpcService<ComplianceServiceImpl>();
app.MapGrpcService<InventoryServiceImpl>();

// Migration step 1: package-shipped replication receiver-side gRPC
// route. Mapped only when the package was wired in above so the
// in-memory test path does not require the gRPC method singleton to
// resolve at startup.
if (packageReplicationConfigured)
{
    app.MapLatticeReplicationGrpcService();
}

// Prometheus scrape endpoint - served at /metrics on the ASP.NET Core
// HTTP port. Anonymous access is fine because the endpoint is only
// reachable on the internal cluster network (not published to the host).
app.MapPrometheusScrapingEndpoint();

// Inbound replication endpoint. Authenticated via
// X-Replication-Token shared secret (see ReplicationTopology.SharedSecret).
app.MapReplicationEndpoint();

await app.RunAsync();

static string? ResolveArg(string[] args, string name, string? fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
        {
            return args[i + 1];
        }
    }
    return fallback;
}

/// <summary>
/// Program entry-point marker, exposed so the test project can reference the
/// host assembly for in-process gRPC contract tests.
/// </summary>
public partial class Program;
