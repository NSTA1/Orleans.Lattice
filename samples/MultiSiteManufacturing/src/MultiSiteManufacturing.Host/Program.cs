using Azure.Data.Tables;
using Orleans.Hosting;
using Orleans.Lattice;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Components;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;

var builder = WebApplication.CreateBuilder(args);

var tableStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureTableStorage")
    ?? "UseDevelopmentStorage=true";

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();

    silo.AddAzureTableGrainStorageAsDefault(options =>
    {
        options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
    });

    silo.AddAzureTableGrainStorage("msmfgGrainState", options =>
    {
        options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
    });

    silo.UseInMemoryReminderService();

    silo.AddLattice((services, name) =>
    {
        services.AddAzureTableGrainStorage(name, options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });
    });
});

builder.Services.AddGrpc();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Federation: one concrete backend per name, both exposed as IFactBackend
// so FederationRouter receives them via the IEnumerable<IFactBackend> ctor.
builder.Services.AddSingleton<BaselineFactBackend>();
builder.Services.AddSingleton<LatticeFactBackend>();
builder.Services.AddSingleton<IFactBackend>(sp => sp.GetRequiredService<BaselineFactBackend>());
builder.Services.AddSingleton<IFactBackend>(sp => sp.GetRequiredService<LatticeFactBackend>());
builder.Services.AddSingleton<FederationRouter>();

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

// gRPC service endpoints are registered in M5. Scaffold only today.

await app.RunAsync();

/// <summary>
/// Program entry-point marker, exposed so the test project can reference the
/// host assembly for in-process gRPC contract tests in later milestones.
/// </summary>
public partial class Program;
