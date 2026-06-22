using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// The web head is Blazor Server: the server process holds the gRPC channel to
// the cluster's state API (native HTTP/2, no gRPC-web proxy needed) and the
// browser renders over the SignalR circuit. Interactive server components host
// the shared UI class library.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// The config backing store, shared connection, and session live in DI. The JSON
// store sits under the server's per-user local app-data folder.
builder.Services.AddExplorerConfiguration();
builder.Services.AddExplorerCatalog();
builder.Services.AddExplorerMetrics();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();
app.MapStaticAssets();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode()
    .AddAdditionalAssemblies(typeof(Orleans.Lattice.Explorer.UI._Imports).Assembly);

app.Run();
