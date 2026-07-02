using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// Diagnostics sample
// ==================
// ILattice.DiagnoseAsync returns a point-in-time health snapshot of a tree:
// shard count, live keys, tombstones, B+ tree depth per shard, and per-shard
// hotness (ops/second). It is an admin-rate API for dashboards, health probes,
// and post-mortem investigation - not for hot-path application logic.
//
// This sample writes some keys, deletes a few (leaving tombstones), then prints
// a deep diagnostic report (deep: true counts tombstones exactly).

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
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grains = host.Services.GetRequiredService<IGrainFactory>();
var tree = grains.GetGrain<ILattice>("inventory");

// Populate the tree, then delete a couple of keys to leave tombstones behind.
Console.WriteLine("Writing 10 keys, then deleting 3 (leaving tombstones)...");
for (var i = 0; i < 10; i++)
{
    await tree.SetAsync($"item-{i:D2}", Encoding.UTF8.GetBytes($"qty-{i}"));
}
await tree.DeleteAsync("item-00");
await tree.DeleteAsync("item-03");
await tree.DeleteAsync("item-07");
Console.WriteLine();

// A deep report walks each shard's leaf chain to count tombstones exactly.
var report = await tree.DiagnoseAsync(deep: true, CancellationToken.None);

Console.WriteLine($"Tree '{report.TreeId}' health snapshot (sampled {report.SampledAt:O}):");
Console.WriteLine($"  Deep report:        {report.Deep}");
Console.WriteLine($"  Shard count:        {report.ShardCount} (of {report.VirtualShardCount} virtual slots)");
Console.WriteLine($"  Total live keys:    {report.TotalLiveKeys}");
Console.WriteLine($"  Total tombstones:   {report.TotalTombstones}");
Console.WriteLine($"  Recent splits:      {report.RecentSplits.Length}");
Console.WriteLine();

Console.WriteLine("Per-shard breakdown (shards with activity only):");
foreach (var shard in report.Shards)
{
    if (shard.LiveKeys == 0 && shard.Tombstones == 0 && shard.Writes == 0 && shard.Reads == 0)
    {
        continue;
    }

    Console.WriteLine(
        $"  shard {shard.ShardIndex}: depth={shard.Depth} rootIsLeaf={shard.RootIsLeaf} " +
        $"live={shard.LiveKeys} tombstones={shard.Tombstones} " +
        $"ratio={shard.TombstoneRatio:F2} reads={shard.Reads} writes={shard.Writes} " +
        $"ops/s={shard.OpsPerSecond:F1}");
}

await host.StopAsync();
