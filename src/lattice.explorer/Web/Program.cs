using Orleans.Lattice.Explorer.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// The web head is Blazor Server: the server process holds the gRPC channel to
// the cluster's state API (native HTTP/2, no gRPC-web proxy needed) and the
// browser renders over the SignalR circuit. Interactive server components host
// the shared UI class library.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

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
