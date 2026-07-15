using System.Text;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.Explorer;
using Orleans.Lattice.Schema;

// Orleans.Lattice.Explorer sample: co-hosts a single-silo cluster, the state /
// auth / schema gRPC admin surfaces, and the embeddable Explorer web console in
// one process, so you can open the console in a browser and walk a live tree,
// its access-control policy, and its schema governance end to end.
//
// The console is the standalone web head's exact code path: AddLatticeExplorerWeb
// registers it and MapLatticeExplorer mounts it, which is all a consumer needs to
// embed the Explorer in their own ASP.NET app. Here it is pointed at the local
// gRPC endpoint through the launcher-friendly bootstrap environment variables.
//
// Three control planes are co-hosted on the one gRPC endpoint - state (Explore),
// auth (Access), and schema (Schema) - so those three console areas are live. The
// console auto-signs-in as a bootstrap administrator (see below), which is what
// makes the Access and Schema areas, gated on an administrator probe, light up
// without a manual login. (The Backups area stays disabled: this sample does not
// co-host the backup gRPC API.)

const string DemoTree = "factory-floor";
const int GrpcPort = 5199;   // h2c gRPC endpoint the console connects to
const int WebPort = 5080;    // HTTP endpoint you browse the console on

// The console auto-signs-in with these (see the environment variables below).
// The username is registered as a bootstrap administrator on the silo, so the
// admin-gated Access and Schema areas accept it; the password is never checked
// by the sample's trusted-token authenticator.
const string AdminUser = "explorer-admin";
const string AdminPassword = "explorer";

// h2c (HTTP/2 without TLS) keeps the sample dependency-free - no dev cert - and
// matches the insecure-loopback-dev transport the console is seeded with below.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// Seed the console's first-run connection through the bootstrap environment
// variables (read by AddLatticeExplorerWeb's environment bootstrap): point it at
// the co-hosted gRPC endpoint and allow the local h2c dev transport.
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_ENDPOINT", $"http://localhost:{GrpcPort}");
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_INSECURE_DEV", "true");

// Auto-sign-in credential applied in memory for this process. The console picks
// it up on startup and attaches it to every admin call, so the Access and Schema
// areas resolve the caller as the bootstrap administrator with no manual login.
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_USERNAME", AdminUser);
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_PASSWORD", AdminPassword);

// Isolate the console's persisted configuration to a sample-owned file and start
// each run from a clean slate. This keeps the sample off the shared per-user
// Explorer config (%LOCALAPPDATA%\Orleans.Lattice.Explorer\config.json), so a
// previous session's saved endpoint can never hijack the demo, and it lets the
// environment bootstrap above re-seed the co-hosted endpoint and admin sign-in on
// every launch.
var sampleConfigPath = Path.Combine(AppContext.BaseDirectory, "explorer-sample-config.json");
if (File.Exists(sampleConfigPath))
{
    File.Delete(sampleConfigPath);
}

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();

// Serve the Explorer UI's packaged static web assets (its stylesheet, favicon,
// and interop script, shipped as an RCL under _content/Orleans.Lattice.Explorer.UI/)
// no matter which environment the sample runs in. WebApplication only auto-maps
// these in Development, so calling it explicitly keeps `dotnet run` styled even
// under the default Production environment.
builder.WebHost.UseStaticWebAssets();
builder.WebHost.ConfigureKestrel(options =>
{
    // gRPC needs HTTP/2; the Blazor Server console needs HTTP/1.1 for its
    // SignalR circuit, so each gets its own port.
    options.ListenLocalhost(GrpcPort, listen => listen.Protocols = HttpProtocols.Http2);
    options.ListenLocalhost(WebPort, listen => listen.Protocols = HttpProtocols.Http1AndHttp2);
});

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.UseInMemoryReminderService();
    silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

    // The read-only state API that backs the console's Explore area.
    silo.AddLatticeStateApi();

    // Membership + authorization give the Access admin area a real control plane
    // to manage and let the fail-closed capability probe succeed. The data-plane
    // default is left permissive so the Explore area works without a sign-in; the
    // reserved control plane (membership + policy) is always governed and only the
    // bootstrap administrator below may manage it.
    silo.AddLatticeMembership();
    silo.AddLatticeAuth(options =>
    {
        options.DefaultEffect = LatticeEffect.Allow;
        options.BootstrapAdministrators.Add(AdminUser);
    });
    silo.AddLatticeAuthApi();

    // Schema enforcement plus its control facade so the Schema admin area is
    // reachable and can govern trees from the console.
    silo.AddLatticeSchemaEnforcement();
    silo.AddLatticeSchemaApi();

    // Trusts the console's auto-applied Basic sign-in: the auth / schema gRPC
    // bridges hand this authenticator the base64(username:password) token and it
    // resolves the caller subject to "explorer-admin", the bootstrap administrator.
    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoBasicAuthenticator>();
});

// The gRPC binding over the state facade. Authorization is disabled here purely
// to keep the sample one-command runnable; a real deployment registers an
// ILatticeStateApiAuthorizer and leaves RequireAuthorization at its secure
// default. Because the sample co-hosts auth, the state API's read-visibility
// filter is active and fail-closed: it only surfaces trees the resolved caller
// may read. Match the console's "Basic base64(user:pass)" sign-in header so the
// state binding resolves the caller as the bootstrap administrator (the same
// scheme the auth and schema bindings use) - otherwise the catalog stays empty.
builder.Services.AddLatticeStateApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});

// The auth and schema control-plane gRPC bindings the Access and Schema areas
// call. Transport authorization is left off (sample-only, so the console needs
// no client certificate), but the silo's own administrator check still runs
// against the resolved caller subject - so the areas only light up because the
// console signs in as the bootstrap administrator. The bridge reads the console's
// "Basic base64(user:pass)" header, hence the Basic credential scheme.
builder.Services.AddLatticeAuthApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});
builder.Services.AddLatticeSchemaApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});

// The embeddable Explorer web console - the one call a consumer makes to host it.
// The sample pins the console's persisted config to its own isolated file so it
// always connects to the co-hosted endpoint seeded above.
builder.Services.AddLatticeExplorerWeb(o => o.ConfigFilePath = sampleConfigPath);

var app = builder.Build();

app.UseAntiforgery();

app.MapLatticeStateApiGrpc();
app.MapLatticeAuthApiGrpc();
app.MapLatticeSchemaApiGrpc();
app.MapLatticeExplorer();

await app.StartAsync();

// Seed a small demo tree so the Explore area has live data. A plain SetAsync
// auto-registers the tree, so it surfaces in the console's catalog with no extra
// wiring.
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>(DemoTree);
for (var i = 0; i < 12; i++)
{
    await tree.SetAsync($"machine-{i:D3}", Encoding.UTF8.GetBytes($"status-{i:D3}"));
}

Console.WriteLine($"Seeded '{DemoTree}' with 12 entries.");
Console.WriteLine($"Silo + state/auth/schema gRPC surface started on http://localhost:{GrpcPort}");
Console.WriteLine($"Explorer console: open http://localhost:{WebPort}/ in a browser.");
Console.WriteLine($"Auto-signed in as bootstrap administrator '{AdminUser}' - the Explore, Access, and Schema areas are all enabled.");
Console.WriteLine("Press Ctrl+C to stop.");

await app.WaitForShutdownAsync();
