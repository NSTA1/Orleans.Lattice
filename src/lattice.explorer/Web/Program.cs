using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Topology;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Explorer.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// The web head is Blazor Server: the server process holds the gRPC channel to
// the cluster's state API (native HTTP/2, no gRPC-web proxy needed) and the
// browser renders over the SignalR circuit. Interactive server components host
// the shared UI class library.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// The config backing store, shared connection, and session live in DI. The JSON
// store sits under the server's per-user local app-data folder, unless a
// launcher overrides the path via LATTICE_EXPLORER_CONFIG.
builder.Services.AddExplorerConfiguration(options =>
{
    var configOverride = Environment.GetEnvironmentVariable(
        Orleans.Lattice.Explorer.Core.Configuration.EnvironmentExplorerBootstrap.ConfigPathVariable);
    if (!string.IsNullOrWhiteSpace(configOverride))
    {
        options.FilePath = configOverride;
    }
});

// Launcher-friendly first-run bootstrap: seed the endpoint (and an optional
// sign-in credential) from environment variables when nothing is persisted yet.
builder.Services.AddExplorerEnvironmentBootstrap();
builder.Services.AddExplorerCatalog();
builder.Services.AddExplorerMetrics();
builder.Services.AddExplorerTopology();
builder.Services.AddExplorerData();
builder.Services.AddExplorerDeadLetter();
builder.Services.AddExplorerHistory();
builder.Services.AddExplorerSession();

// The web head persists UI preferences to the browser's localStorage (Data
// Protection-encrypted), overriding the in-memory fallback backing store.
builder.Services.AddScoped<
    Orleans.Lattice.Explorer.Core.Session.IUiPreferenceBackingStore,
    ProtectedLocalStoragePreferenceBackingStore>();

// Authentication. The credential rests in an HttpOnly + Secure cookie encrypted
// with Data Protection (no browser storage); the login dialog posts to the
// server endpoints below so the password never crosses the SignalR circuit.
builder.Services.AddDataProtection();
builder.Services.AddHttpContextAccessor();
builder.Services.AddSingleton<ICredentialStore, CookieCredentialStore>();
builder.Services.AddSingleton(new ExplorerAuthUiOptions { UseServerFormPost = true });
builder.Services.AddExplorerAuth();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();
app.MapStaticAssets();

// Server-side sign-in / sign-out endpoints. The login form posts here so the
// password is handled on the server and stored in the encrypted cookie rather
// than round-tripped over the circuit. Antiforgery is validated on each POST
// (the forms embed a RequestVerificationToken) to block login / logout CSRF
// against the shared process-global auth session.
app.MapExplorerAuthEndpoints();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode()
    .AddAdditionalAssemblies(typeof(Orleans.Lattice.Explorer.UI._Imports).Assembly);

app.Run();
