using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// TreeRegistry
// ---------------------------------------------------------------------------
// Lattice keeps an internal registry of every user tree. A tree registers
// itself automatically on its first write, so you can discover all trees in a
// cluster - and their per-tree configuration - without maintaining a list
// yourself.
//
// This sample:
//   1. declares per-tree option overrides at startup (ConfigureLattice),
//   2. creates a few trees by writing to them,
//   3. enumerates every registered tree with GetAllTreeIdsAsync,
//   4. checks presence with TreeExistsAsync, and
//   5. prints each tree's effective (possibly overridden) options.
//
// See docs/lattice/tree-registry.md.
// ---------------------------------------------------------------------------

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

        // Per-tree option overrides. These are resolved via
        // IOptionsMonitor<LatticeOptions>.Get(treeName) and take precedence
        // over the global defaults for that tree only. "sessions" gets no
        // override, so it falls back to the global defaults.
        silo.ConfigureLattice("orders", o => o.CacheTtl = TimeSpan.FromSeconds(30));
        silo.ConfigureLattice("audit", o => o.TombstoneGracePeriod = TimeSpan.FromDays(14));
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

// Create three trees simply by writing one key to each. The first write to a
// brand-new tree registers it in the registry before the data write proceeds.
var treesToCreate = new[] { "orders", "audit", "sessions" };
Console.WriteLine("== Creating trees (first write auto-registers each) ==");
foreach (var id in treesToCreate)
{
    await grainFactory.GetGrain<ILattice>(id).SetAsync("seed", Encoding.UTF8.GetBytes("hello"));
    Console.WriteLine($"  wrote to '{id}'");
}
Console.WriteLine();

// Enumerate every registered tree. GetAllTreeIdsAsync can be called from ANY
// tree grain - the registry is shared, so the handle used here is arbitrary.
var registryProbe = grainFactory.GetGrain<ILattice>("orders");
var allIds = await registryProbe.GetAllTreeIdsAsync();

Console.WriteLine("== Registered trees (GetAllTreeIdsAsync) ==");
foreach (var id in allIds.OrderBy(x => x, StringComparer.Ordinal))
{
    Console.WriteLine($"  - {id}");
}
Console.WriteLine();

// Existence checks: one tree we created, one we never touched.
Console.WriteLine("== Existence checks (TreeExistsAsync) ==");
var ordersExists = await grainFactory.GetGrain<ILattice>("orders").TreeExistsAsync();
var ghostExists = await grainFactory.GetGrain<ILattice>("never-written").TreeExistsAsync();
Console.WriteLine($"  'orders' exists       = {ordersExists}");
Console.WriteLine($"  'never-written' exists = {ghostExists}");
Console.WriteLine();

// Per-tree configuration overrides. The options monitor returns the effective
// LatticeOptions for a given tree name, folding in any ConfigureLattice
// override. This is how you see which trees carry non-default config.
var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<LatticeOptions>>();

Console.WriteLine("== Per-tree config overrides ==");
foreach (var id in treesToCreate)
{
    var opts = optionsMonitor.Get(id);
    Console.WriteLine(
        $"  {id,-9} CacheTtl={opts.CacheTtl,-10} TombstoneGracePeriod={opts.TombstoneGracePeriod}");
}
Console.WriteLine();
Console.WriteLine("Note: 'orders' overrides CacheTtl, 'audit' overrides TombstoneGracePeriod,");
Console.WriteLine("      'sessions' shows the global defaults.");

await host.StopAsync();
