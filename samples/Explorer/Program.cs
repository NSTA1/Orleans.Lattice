using System.Text;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Web;

// Orleans.Lattice.Explorer sample: co-hosts a single-silo cluster, the read-only
// state-API gRPC surface, and the embeddable Explorer web console in one process,
// so you can open the console in a browser and walk a live tree end to end.
//
// The console is the standalone web head's exact code path: AddLatticeExplorerWeb
// registers it and MapLatticeExplorer mounts it, which is all a consumer needs to
// embed the Explorer in their own ASP.NET app. Here it is pointed at the local
// gRPC endpoint through the launcher-friendly bootstrap environment variables.

const string DemoTree = "factory-floor";
const int GrpcPort = 5199;   // h2c gRPC endpoint the console connects to
const int WebPort = 5080;    // HTTP endpoint you browse the console on

// h2c (HTTP/2 without TLS) keeps the sample dependency-free - no dev cert - and
// matches the insecure-loopback-dev transport the console is seeded with below.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// Seed the console's first-run connection through the bootstrap environment
// variables (read by AddLatticeExplorerWeb's environment bootstrap): point it at
// the co-hosted gRPC endpoint and allow the local h2c dev transport.
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_ENDPOINT", $"http://localhost:{GrpcPort}");
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_INSECURE_DEV", "true");

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
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
});

// The gRPC binding over the state facade. Authorization is disabled here purely
// to keep the sample one-command runnable; a real deployment registers an
// ILatticeStateApiAuthorizer and leaves RequireAuthorization at its secure
// default (and enables the auth, backup, and schema gRPC APIs to light up the
// Access, Backups, and Schema admin areas).
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = false);

// The embeddable Explorer web console - the one call a consumer makes to host it.
builder.Services.AddLatticeExplorerWeb();

var app = builder.Build();

app.UseAntiforgery();

app.MapLatticeStateApiGrpc();
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
Console.WriteLine($"Silo + state-API gRPC surface started on http://localhost:{GrpcPort}");
Console.WriteLine($"Explorer console: open http://localhost:{WebPort}/ in a browser.");
Console.WriteLine("Press Ctrl+C to stop.");

await app.WaitForShutdownAsync();
