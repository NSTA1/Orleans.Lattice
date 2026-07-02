using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// =============================================================================
// Snapshots
// -----------------------------------------------------------------------------
// Demonstrates SnapshotAsync: a point-in-time copy of a whole tree into a new
// destination tree. Snapshots are useful for backups, read-only analytics
// forks, or cloning a dataset for experimentation.
//
// This sample uses SnapshotMode.Offline: the source tree is locked shard by
// shard during the copy, producing a strictly consistent point-in-time image.
// Switching to SnapshotMode.Online is a one-line change - the source then stays
// readable and writable throughout, with live mutations mirrored to the
// destination (see the README for the trade-off).
//
// SnapshotAsync starts a crash-safe, timer-driven coordinator that copies one
// shard per tick, so we poll IsSnapshotCompleteAsync until it finishes. A tree
// defaults to 64 physical shards, so the copy takes a couple of minutes even
// for a tiny dataset - snapshot cost scales with shard count, not key count.
// =============================================================================

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

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);
static string Text(byte[] b) => Encoding.UTF8.GetString(b);

// Poll until the snapshot coordinator reports completion, printing a heartbeat
// so the (multi-shard, multi-second) copy shows visible progress.
static async Task WaitForSnapshotAsync(ILattice source)
{
    var sw = Stopwatch.StartNew();
    while (!await source.IsSnapshotCompleteAsync())
    {
        Console.Write(".");
        await Task.Delay(1000);
    }
    Console.WriteLine($" done in {sw.Elapsed.TotalSeconds:F0}s.");
}

// Count every live key in a tree by scanning it.
static async Task<int> CountByScanAsync(ILattice tree)
{
    var n = 0;
    await foreach (var _ in tree.ScanKeysAsync())
    {
        n++;
    }
    return n;
}

var source = grainFactory.GetGrain<ILattice>("orders");

// --- Step 1: populate the source tree --------------------------------------
const int keyCount = 12;
Console.WriteLine($"Seeding source tree 'orders' with {keyCount} keys...");
for (var i = 0; i < keyCount; i++)
{
    await source.SetAsync($"order:{i:D3}", Value($"amount={i * 10}"));
}
Console.WriteLine($"  source count = {await source.CountAsync()}");
Console.WriteLine();

// --- Step 2: take an offline point-in-time snapshot -------------------------
// The source is locked shard-by-shard during the copy, so the destination is a
// strictly consistent image of the source at this instant.
Console.Write("Offline snapshot: orders -> orders-backup ");
await source.SnapshotAsync("orders-backup", SnapshotMode.Offline);
await WaitForSnapshotAsync(source);
Console.WriteLine();

// --- Step 3: verify the copy matches the source -----------------------------
var backup = grainFactory.GetGrain<ILattice>("orders-backup");
var backupCount = await CountByScanAsync(backup);
var sampleBackup = await backup.GetAsync("order:005");
Console.WriteLine("Verifying the snapshot:");
Console.WriteLine($"  backup live-key count = {backupCount} (expected {keyCount})");
Console.WriteLine($"  backup[order:005]     = \"{Text(sampleBackup!)}\"");
Console.WriteLine($"  source readable again = {await source.GetAsync("order:005") is not null}");
Console.WriteLine();

// --- Step 4: the snapshot is an independent tree ----------------------------
// Editing the backup does not touch the source, and writing to the source does
// not touch the backup. They diverge from the shared point-in-time origin.
await backup.SetAsync("order:005", Value("edited-in-backup"));
await source.SetAsync("order:999", Value("new-in-source"));

var sourceStill005 = await source.GetAsync("order:005");
var backupHas999 = await backup.GetAsync("order:999");
Console.WriteLine("Independence after editing each tree separately:");
Console.WriteLine($"  backup[order:005] = \"{Text((await backup.GetAsync("order:005"))!)}\" (edited)");
Console.WriteLine($"  source[order:005] = \"{Text(sourceStill005!)}\" (unchanged)");
Console.WriteLine($"  source[order:999] = \"{Text((await source.GetAsync("order:999"))!)}\" (new)");
Console.WriteLine($"  backup[order:999] = {(backupHas999 is null ? "<absent>" : Text(backupHas999))} (not in the snapshot)");
Console.WriteLine();

Console.WriteLine("Done: the offline snapshot produced an independent point-in-time copy.");

await host.StopAsync();
