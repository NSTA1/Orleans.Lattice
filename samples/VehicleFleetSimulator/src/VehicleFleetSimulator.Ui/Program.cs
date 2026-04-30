using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using VehicleFleetSimulator.Ui;
using VehicleFleetSimulator.Ui.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

var apiBase = builder.Configuration["Api:BaseAddress"]
    ?? builder.HostEnvironment.BaseAddress;

builder.Services.AddSingleton(new Uri(apiBase));
builder.Services.AddSingleton<FleetState>();
builder.Services.AddSingleton<SimulationConfigClient>(sp => new SimulationConfigClient(
    sp.GetRequiredService<Uri>()));
builder.Services.AddSingleton<FleetAdminClient>(sp => new FleetAdminClient(
    sp.GetRequiredService<Uri>()));
builder.Services.AddSingleton<FleetStreamClient>(sp => new FleetStreamClient(
    sp.GetRequiredService<Uri>(),
    sp.GetRequiredService<FleetState>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<FleetStreamClient>()));

await builder.Build().RunAsync();

