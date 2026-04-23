using Azure.Data.Tables;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
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

var builder = WebApplication.CreateBuilder(args);

// M13: cluster-aware bootstrap. The sample now launches as one of two
// independent Orleans clusters — "forge" and "heattreat" — selected via
// the --cluster command-line argument. Each cluster has its own ClusterId,
// its own Azurite instance, and its own HTTP port range. A per-cluster
// overlay file (appsettings.cluster.{name}.json) supplies connection
// strings, ports, and the replication topology; it's merged on top of
// appsettings.json so shared defaults still apply.
var clusterName = ResolveArg(args, "--cluster", builder.Configuration["CLUSTER_NAME"]) ?? "forge";
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

// M14: when running under Docker Compose, ASPNETCORE_URLS is set by the
// container env (typically "http://+:8080") and must be honoured as-is —
// binding to "localhost" inside a container only binds the loopback
// interface and the host-side port publish (5001..5004) never reaches
// the app. For the legacy host-process path (run.ps1 in -Legacy mode,
// or plain `dotnet run`), ASPNETCORE_URLS is unset and we fall back to
// the per-A/B httpPort from the cluster overlay.
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

// M13: load the replication topology once and publish as a singleton
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

builder.Host.UseOrleans(silo =>
{
    // M13: register the outgoing grain-call filter so every ILattice
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
    }
});

builder.Services.AddGrpc();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Federation: one concrete backend per name, each wrapped in a
// ChaosFactBackend decorator so IBackendChaosGrain (plan §4.3 Tier 2)
// can inject jitter, transient failures, and write amplification.
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

// M12b: CRDT-typed grain state backed by Orleans.Lattice. Singleton
// because it wraps a single ILattice grain reference and holds no
// per-call state.
builder.Services.AddSingleton<PartCrdtStore>();

// M12c: drains this silo's CRDT shadow prefix back into the shared
// prefix when the simulated inter-silo partition heals.
builder.Services.AddHostedService<PartitionHealHostedService>();

// M12d: keeps a lightweight B+ tree index of {site}/{stage}/{serial}
// entries so the "Parts by site" page can render a live per-site
// inventory via a range scan on ILattice. The index subscribes to
// FederationRouter.FactRouted and writes one entry per Fact — every
// fact type carries a Site, so inspection-only sites (Stuttgart CMM
// Lab) appear alongside process-step sites.
builder.Services.AddSingleton<SiteActivityIndex>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SiteActivityIndex>());

builder.Services.AddSingleton<DashboardBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<DashboardBroadcaster>());

// M13: seeder runs on exactly one silo of exactly one cluster so the
// two clusters don't race and produce duplicate keys. Suppressed in
// the Testing environment so contract tests start against empty state.
//
// Gating precedence:
//   1. Configuration key "Seeder:Enabled" (env var `Seeder__Enabled`)
//      wins outright when present — `true` forces seeding on this
//      silo, `false` forces it off. Useful in docker-compose / CI
//      where the topology is declarative.
//   2. Otherwise, the default heuristic runs the seeder only on the
//      primary silo of the "forge" cluster — the convention the
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
            && string.Equals(clusterName, "forge", StringComparison.OrdinalIgnoreCase);
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

// M13: inbound replication endpoint. Authenticated via
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
