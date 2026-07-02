using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// OnlineReshard
// ---------------------------------------------------------------------------
// ILattice.ReshardAsync grows a tree's physical shard count ONLINE: the tree
// keeps serving reads and writes throughout, with no global cutover lock and no
// maintenance window. Resharding spreads the key space across more independent
// write paths - it is grow-only.
//
// This sample:
//   1. writes a set of keys and records the starting shard count,
//   2. calls ReshardAsync to grow the shard count,
//   3. polls IsReshardCompleteAsync until the migration finishes, and
//   4. proves every key is still readable and the shard count actually grew.
//
// See docs/lattice/online-reshard.md.
// ---------------------------------------------------------------------------

const int TargetShardCount = 72;

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

        // Dispatch more concurrent splits per migration tick so the demo
        // finishes quickly. Higher values migrate faster at the cost of more
        // drain I/O; the default is 4.
        silo.ConfigureLattice(o => o.MaxConcurrentMigrations = 16);
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>("catalogue");

// Seed the tree with keys we will re-read after the reshard to prove no data
// was lost during the online migration.
const int keyCount = 24;
Console.WriteLine($"== Writing {keyCount} keys ==");
for (var i = 0; i < keyCount; i++)
{
    await tree.SetAsync($"key/{i:D3}", Encoding.UTF8.GetBytes($"value-{i}"));
}
Console.WriteLine($"  wrote key/000 .. key/{keyCount - 1:D3}");
Console.WriteLine();

var startShards = await DistinctShardsAsync(tree);
Console.WriteLine($"Starting physical shard count: {startShards}");
Console.WriteLine($"Resharding online to {TargetShardCount} shards...");
Console.WriteLine();

// Kick off the reshard. It returns as soon as the intent is durably persisted;
// the migration then proceeds in the background, split by split.
await tree.ReshardAsync(TargetShardCount);

// While the reshard runs, the tree stays fully available. Issue a live write to
// demonstrate that reads and writes are served throughout the migration.
await tree.SetAsync("key/live-during-reshard", Encoding.UTF8.GetBytes("written mid-migration"));
Console.WriteLine("  wrote key/live-during-reshard WHILE the migration was in flight");

// Poll until the coordinator reports completion (bounded so the sample never
// hangs if something is misconfigured). Keep the loop light - a heavy
// DiagnoseAsync fan-out on every tick would compete with the migration.
var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(4);
var tick = 0;
while (!await tree.IsReshardCompleteAsync())
{
    if (DateTime.UtcNow > deadline)
    {
        Console.WriteLine("  [WARN] reshard did not complete within the timeout.");
        break;
    }

    if (++tick % 5 == 0)
    {
        Console.WriteLine($"    ...migrating (distinct shards so far: {await DistinctShardsAsync(tree)})");
    }

    await Task.Delay(TimeSpan.FromSeconds(2));
}
Console.WriteLine();

var endShards = await DistinctShardsAsync(tree);
Console.WriteLine($"Final physical shard count: {endShards}");
Console.WriteLine();

// Verify every original key - plus the one written mid-migration - is intact.
Console.WriteLine("== Verifying all keys survived the reshard ==");
var missing = 0;
for (var i = 0; i < keyCount; i++)
{
    var value = await tree.GetAsync($"key/{i:D3}");
    if (value is null || Encoding.UTF8.GetString(value) != $"value-{i}")
    {
        missing++;
    }
}
var liveValue = await tree.GetAsync("key/live-during-reshard");

Console.WriteLine($"  original keys intact : {keyCount - missing}/{keyCount}");
Console.WriteLine($"  mid-migration write   : {(liveValue is null ? "<lost>" : Encoding.UTF8.GetString(liveValue))}");
Console.WriteLine();

Console.WriteLine(
    missing == 0 && endShards > startShards
        ? $"[OK] shard count grew {startShards} -> {endShards} with zero data loss and no downtime."
        : "[FAIL] reshard did not meet expectations.");

await host.StopAsync();

// Reports the number of DISTINCT physical shards the tree's key space is
// currently spread across, read from the effective ShardMap. forceRefresh
// busts the activation's cached routing snapshot so growth is observed
// immediately. This is the count ReshardAsync grows.
static async Task<int> DistinctShardsAsync(ILattice tree)
{
    var routing = await tree.GetRoutingAsync(forceRefresh: true);
    return routing.Map.GetPhysicalShardIndices().Count;
}
