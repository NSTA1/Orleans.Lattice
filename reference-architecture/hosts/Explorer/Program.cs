using Orleans.Lattice.Explorer.Entra;
using Orleans.Lattice.Explorer.Web;

// ---------------------------------------------------------------------------
// Reference-architecture Explorer head.
//
// A standalone operator web console (Blazor Server) that is NOT co-hosted with a
// silo: it connects, as a gRPC / gRPC-web client, to a remote State + Auth gRPC
// endpoint published by the region's silo. It is the same embeddable code path
// the Orleans.Lattice.Explorer.Web hosting library exposes, driven here purely by
// configuration so the standalone head and any co-hosted explorer cannot drift.
//
// The console's first-run connection is seeded by the explorer's own environment
// bootstrap (LATTICE_EXPLORER_ENDPOINT / LATTICE_EXPLORER_INSECURE_DEV /
// LATTICE_EXPLORER_USERNAME / LATTICE_EXPLORER_PASSWORD / LATTICE_EXPLORER_CONFIG),
// so the remote endpoint and any auto-sign-in are configuration, never code.
//
// Auth: the console offers an interactive Microsoft Entra sign-in when Entra is
// enabled (AddExplorerEntraAuth); the acquired token is attached to its calls and
// re-validated by the silo's Entra authenticator. The local compose harness
// disables Entra (a documented dev bypass) and uses the built-in Basic sign-in
// against the dev cluster.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Serve the Explorer UI's packaged static web assets in every environment (the
// framework only auto-maps these in Development).
builder.WebHost.UseStaticWebAssets();

var entraEnabled = config.GetValue("Entra:Enabled", false);

// Persist the explorer's JSON config to a writable location. The chiseled,
// non-root container has no writable per-user app-data directory, so default the
// backing store to a writable path (overridable via Explorer:ConfigFilePath or
// the LATTICE_EXPLORER_CONFIG environment variable).
var configFilePath = config["Explorer:ConfigFilePath"]
    ?? Path.Combine(Path.GetTempPath(), "lattice-explorer", "config.json");
Directory.CreateDirectory(Path.GetDirectoryName(configFilePath)!);

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.ConfigFilePath = configFilePath;
    options.EnableSchemaArea = config.GetValue("Explorer:EnableSchemaArea", false);
});

// Interactive Entra sign-in provider, offered alongside the built-in Basic
// provider when Entra is enabled.
if (entraEnabled)
{
    var tenantId = config["Entra:TenantId"]
        ?? throw new InvalidOperationException("Entra:TenantId is required when Entra:Enabled is true.");
    var clientId = config["Entra:ClientId"]
        ?? throw new InvalidOperationException("Entra:ClientId is required when Entra:Enabled is true.");

    builder.Services.AddExplorerEntraAuth(options =>
    {
        options.TenantId = tenantId;
        options.ClientId = clientId;
        options.Authority = config["Entra:Authority"] ?? $"https://login.microsoftonline.com/{tenantId}";
        options.UseDeviceCode = config.GetValue("Entra:UseDeviceCode", false);
    });
}

var app = builder.Build();

// TLS is terminated at the platform ingress (ACA / the compose front); the
// container itself serves plain HTTP so health probes and the SignalR circuit do
// not hit an in-container HTTPS redirect. No UseHttpsRedirection here by design.
app.UseAntiforgery();

app.MapLatticeExplorer();
app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
