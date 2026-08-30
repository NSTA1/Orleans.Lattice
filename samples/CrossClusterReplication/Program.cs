using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Samples.CrossClusterReplication;

// Cross-cluster replication ships batches over grpc-dotnet. For a loopback dev
// sample we talk plaintext HTTP/2 (h2c): each site binds Kestrel to HTTP/2 with
// no certificate, and grpc-dotnet speaks h2c by prior knowledge over an http://
// address. Production uses https:// instead. No process-global switch is
// involved either way.

// Two mirror-image sites. Site A ships to B; B ships to A (active-active).
// Distinct Orleans ports let both clusters live in one process; distinct gRPC
// ports give each its own inbound replication endpoint.
var siteA = new SiteConfig(
    ClusterId: "site-a", SiloPort: 11111, GatewayPort: 30000,
    GrpcPort: 17001, PeerClusterId: "site-b", PeerGrpcPort: 17002);
var siteB = new SiteConfig(
    ClusterId: "site-b", SiloPort: 11112, GatewayPort: 30001,
    GrpcPort: 17002, PeerClusterId: "site-a", PeerGrpcPort: 17001);

var appA = SiteFactory.Build(siteA);
var appB = SiteFactory.Build(siteB);

Console.WriteLine("Starting two independent Orleans clusters (site-a, site-b)...");
await appA.StartAsync();
await appB.StartAsync();
Console.WriteLine("Both clusters ready and peered over gRPC.\n");

// Resolve the replicated "orders" tree on each cluster. Same tree name, two
// physically separate clusters, kept convergent only by replication.
var treeA = appA.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SiteFactory.TreeName);
var treeB = appB.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(SiteFactory.TreeName);

const string key = "order/1001";

Console.WriteLine("== Before ==");
var beforeB = await treeB.GetAsync(key);
Console.WriteLine($"  site-b sees '{key}' = {Render(beforeB)}");

// Write ONLY on site A. Nothing is written directly to site B.
Console.WriteLine("\n== Writing on site-a only ==");
await treeA.SetAsync(key, System.Text.Encoding.UTF8.GetBytes("CONFIRMED"));
Console.WriteLine($"  site-a wrote '{key}' = CONFIRMED");

// Wait for the value to converge on site B purely via cross-cluster shipping.
Console.WriteLine("\n== Waiting for convergence on site-b (no direct write) ==");
var converged = await WaitForAsync(treeB, key, "CONFIRMED", TimeSpan.FromSeconds(30));

var afterB = await treeB.GetAsync(key);
Console.WriteLine($"  site-b now sees '{key}' = {Render(afterB)}");

Console.WriteLine();
Console.WriteLine(converged
    ? "[OK] the write made on site-a converged onto site-b across clusters."
    : "[FAIL] the write did not converge within the timeout.");

await appA.StopAsync();
await appB.StopAsync();

static string Render(byte[]? value) =>
    value is null ? "(absent)" : System.Text.Encoding.UTF8.GetString(value);

// Polls the target tree until the key holds the expected value or the budget
// elapses, printing a dot per poll so progress is visible.
static async Task<bool> WaitForAsync(ILattice tree, string key, string expected, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        var current = await tree.GetAsync(key);
        if (current is not null && System.Text.Encoding.UTF8.GetString(current) == expected)
        {
            Console.WriteLine("  converged.");
            return true;
        }

        Console.Write("  .");
        await Task.Delay(TimeSpan.FromMilliseconds(500));
    }

    Console.WriteLine();
    return false;
}
