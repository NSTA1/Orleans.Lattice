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

var builder = WebApplication.CreateBuilder(args);

var useInMemoryStorage = builder.Environment.IsEnvironment("Testing")
    || builder.Configuration.GetValue<bool>("Orleans:UseInMemoryStorage");

var tableStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureTableStorage")
    ?? "UseDevelopmentStorage=true";

// Multi-silo localhost cluster (plan §M12a). The sample ships with two
// silo identities, "a" and "b", which bind distinct Orleans silo/gateway
// ports and distinct HTTP ports. When not using in-memory storage, both
// silos cluster via shared Azure Table Storage (Azurite by default) and
// share all grain state + the Lattice fact store — so writes on silo A
// are observable from silo B's Blazor UI. In-memory mode forces
// single-silo (UseLocalhostClustering with no peer) because in-memory
// clustering doesn't span processes.
var siloId = ResolveSiloId(args, builder.Configuration);
var isPrimarySilo = string.Equals(siloId, "a", StringComparison.OrdinalIgnoreCase);
var siloPort = isPrimarySilo ? 11111 : 11112;
var gatewayPort = isPrimarySilo ? 30000 : 30001;
var httpPort = isPrimarySilo ? 5001 : 5002;

builder.WebHost.UseUrls($"http://localhost:{httpPort}");

// When the host is launched via `dotnet <dll>` (as run.ps1 does, to avoid
// the shared-bin/obj race of two concurrent `dotnet run` invocations), the
// default environment is Production and ASP.NET Core does not auto-wire the
// Static Web Assets manifest. Without this call, MapStaticAssets serves
// _framework/blazor.web.js as a 200 with 0 bytes — the Blazor bootstrap
// script loads but does nothing, so the interactive SignalR circuit never
// forms and all @onclick handlers (Chaos flyout, Race, Fix) are dead.
builder.WebHost.UseStaticWebAssets();

builder.Services.AddSingleton(new SiloIdentity(siloId, isPrimarySilo));

builder.Host.UseOrleans(silo =>
{
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
            o.ClusterId = "msmfg-cluster";
            o.ServiceId = "msmfg-service";
        });

        silo.ConfigureEndpoints(siloPort, gatewayPort);

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

// Seeder runs only on the primary silo (A) so a secondary-silo restart
// doesn't try to re-seed over the shared Azure Table Storage account.
// Also suppressed in the Testing environment so contract tests start
// against empty state.
if (!builder.Environment.IsEnvironment("Testing") && isPrimarySilo)
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

await app.RunAsync();

static string ResolveSiloId(string[] args, IConfiguration config)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (string.Equals(args[i], "--silo-id", StringComparison.OrdinalIgnoreCase))
        {
            return args[i + 1];
        }
    }
    return config["SILO_ID"]
        ?? Environment.GetEnvironmentVariable("MSMFG_SILO_ID")
        ?? "a";
}

/// <summary>
/// Program entry-point marker, exposed so the test project can reference the
/// host assembly for in-process gRPC contract tests.
/// </summary>
public partial class Program;
