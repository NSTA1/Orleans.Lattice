using System.Text;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

// Orleans.Lattice.Api.State sample: a console tree-explorer that co-hosts a
// single-silo Orleans cluster with the read-only state API gRPC surface, then
// connects to it over a real gRPC channel as an external client would and walks
// the full explorer journey: discover -> structure -> scan -> tail live changes.
//
// The whole thing runs in one process for convenience, but the client talks to
// the server strictly over gRPC using only the package's public surface
// (LatticeStateApiGrpcClient + the wire DTOs), so it doubles as a copy-paste
// reference for a standalone dashboard or CLI.

const string DemoTree = "factory-floor";
const int Port = 5199;

// h2c (HTTP/2 without TLS) keeps the sample dependency-free - no dev cert.
// Kestrel binds HttpProtocols.Http2 with no certificate below, and grpc-dotnet
// speaks h2c by prior knowledge over an http:// address, so no process-global
// AppContext switch is needed.

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(Port, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.UseInMemoryReminderService();
    silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

    // The read-only state API. Sourced strictly from aggregates the core
    // library already maintains, so it adds no cost to the read/write path.
    silo.AddLatticeStateApi();
});

// The gRPC binding over the facade. Authorization is disabled here purely to
// keep the sample one-command runnable; a real deployment registers an
// ILatticeStateApiAuthorizer and leaves RequireAuthorization at its secure
// default.
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = false);

var app = builder.Build();
app.MapLatticeStateApiGrpc();
await app.StartAsync();

Console.WriteLine("Silo + state-API gRPC surface started on http://localhost:" + Port);

// Seed a small demo tree. A plain SetAsync auto-registers the tree, so it
// surfaces in the discovery catalog with no extra wiring.
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>(DemoTree);
for (var i = 0; i < 12; i++)
{
    await tree.SetAsync($"machine-{i:D3}", Encoding.UTF8.GetBytes($"status-{i:D3}"));
}

Console.WriteLine($"Seeded '{DemoTree}' with 12 entries.\n");

// Connect as an external client would: a GrpcChannel + the public client.
using var channel = GrpcChannel.ForAddress($"http://localhost:{Port}");
var client = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), app.Services);

// 1) Discover the trees in the cluster.
Console.WriteLine("== Discover ==");
var catalog = await client.ListTreesAsync(new CatalogRequest { PageSize = 50 });
foreach (var entry in catalog.Entries)
{
    Console.WriteLine($"  tree '{entry.TreeId}'  shards={entry.ShardCount}  lifecycle={entry.Lifecycle}");
}

// 2) Render the tree's structure (shard roots + their subtree key counts).
Console.WriteLine("\n== Structure ==");
var structure = await client.GetTreeStructureAsync(new StructureRequest { TreeId = DemoTree });
foreach (var root in structure.Roots)
{
    Console.WriteLine($"  shard {root.ShardIndex}: {root.Kind}  liveKeys={root.SubtreeKeyCount}  children={root.ChildCount}");
}

// 3) Scan a key range, paging the snapshot-isolated cursor to completion.
Console.WriteLine("\n== Scan ==");
var total = 0;
string? token = null;
do
{
    var page = await client.ScanEntriesAsync(new EntryScanRequest
    {
        TreeId = DemoTree,
        PageSize = 5,
        ContinuationToken = token,
    });
    foreach (var record in page.Entries)
    {
        Console.WriteLine($"  {record.Key}  ({record.ValueLength} bytes)");
        total++;
    }

    token = page.ContinuationToken;
}
while (!string.IsNullOrEmpty(token));

Console.WriteLine($"  scanned {total} entries.");

// 4) Tail live changes: subscribe, then make a write and watch it surface.
Console.WriteLine("\n== Tail (live changes) ==");
using var tailCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
var tail = Task.Run(async () =>
{
    try
    {
        await foreach (var change in client.ObserveChangesAsync(
            new StateObserveRequest { TreeId = DemoTree },
            tailCts.Token))
        {
            Console.WriteLine($"  change: {change.Kind} key='{change.Key}'");
            return;
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("  (no change observed before timeout)");
    }
});

await Task.Delay(300);
await tree.SetAsync("machine-999", Encoding.UTF8.GetBytes("hot-swapped"));
await tail;
tailCts.Cancel();

Console.WriteLine("\nExplorer journey complete.");
await app.StopAsync();
