using Azure.Data.Tables;
using Orleans.Hosting;
using Orleans.Lattice;
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

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.UseInMemoryReminderService();

    if (useInMemoryStorage)
    {
        silo.AddMemoryGrainStorageAsDefault();
        silo.AddMemoryGrainStorage("msmfgGrainState");
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));
    }
    else
    {
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
// can inject jitter, transient failures, and write amplification. The
// router consumes IEnumerable<IFactBackend> and sees only the decorated
// instances; the undecorated singletons remain available for code that
// needs a direct reference (e.g. future diagnostic endpoints).
builder.Services.AddSingleton<BaselineFactBackend>();
builder.Services.AddSingleton<LatticeFactBackend>();
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<BaselineFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<LatticeFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<FederationRouter>();

// Operator action surface: OperatorClock is a singleton (monotonic HLC
// shared across all circuits & gRPC handlers); OperatorActions is scoped
// so each Blazor circuit gets a fresh instance but uses the shared clock.
builder.Services.AddSingleton<OperatorClock>();
builder.Services.AddScoped<OperatorActions>();

// Dashboard broadcaster: subscribes to FederationRouter events and
// fans out PartSummaryUpdate / ChaosOverview / DivergenceEvent messages
// to Blazor components and the WatchDivergence gRPC stream via
// per-subscriber Channel<T>. Registered as a singleton so DI consumers
// (Razor components, gRPC services) and the hosted-service lifecycle
// (Start/Stop subscription hooks) share the same instance. Registered
// as a hosted service unconditionally — the WatchDivergence gRPC
// contract tests depend on the broadcaster's FactRouted subscription
// being active.
builder.Services.AddSingleton<DashboardBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<DashboardBroadcaster>());

// Bulk-load seeder: populates ~50 parts against an empty storage account,
// no-ops on subsequent starts via IInventorySeedStateGrain. Suppressed in
// the Testing environment so contract tests start against empty state.
if (!builder.Environment.IsEnvironment("Testing"))
{
    builder.Services.AddHostedService<InventorySeeder>();
}

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<FactIngressServiceImpl>();
app.MapGrpcService<SiteControlServiceImpl>();
app.MapGrpcService<ComplianceServiceImpl>();
app.MapGrpcService<InventoryServiceImpl>();

await app.RunAsync();

/// <summary>
/// Program entry-point marker, exposed so the test project can reference the
/// host assembly for in-process gRPC contract tests in later milestones.
/// </summary>
public partial class Program;
