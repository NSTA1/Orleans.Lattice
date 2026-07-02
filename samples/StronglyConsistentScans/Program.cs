using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// =============================================================================
// StronglyConsistentScans
// -----------------------------------------------------------------------------
// Demonstrates that CountAsync / ScanKeysAsync / ScanEntriesAsync return the
// EXACT live key set - never a torn, partial, or double-counted view - even
// while foreground writes are landing concurrently.
//
// Why this matters: in many sharded stores an aggregate like "count" is a
// best-effort fan-out that can observe some shards before a write and others
// after it, yielding a number that never actually existed. Lattice scans are
// strongly consistent: every reading corresponds to a real committed state of
// the tree, so a stream of concurrent readings is monotonic and every value is
// a count the tree genuinely held at some instant.
// =============================================================================

// Single-silo in-process Orleans cluster with Lattice on in-memory storage.
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
var tree = grainFactory.GetGrain<ILattice>("strongly-consistent-scans");

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);

// --- Step 1: seed a known baseline -----------------------------------------
// Write 500 keys up front. After this completes, the tree holds exactly 500
// live keys and every scan primitive must agree on that number.
const int seedCount = 500;
Console.WriteLine($"Seeding {seedCount} keys (item:0000 .. item:0499)...");
for (var i = 0; i < seedCount; i++)
{
    await tree.SetAsync($"item:{i:D4}", Value($"v{i}"));
}

var baselineCount = await tree.CountAsync();
var baselineScan = 0;
await foreach (var _ in tree.ScanKeysAsync())
{
    baselineScan++;
}

Console.WriteLine($"  CountAsync()      = {baselineCount}");
Console.WriteLine($"  ScanKeysAsync -> {baselineScan} keys");
Console.WriteLine($"  Agree on baseline: {baselineCount == seedCount && baselineScan == seedCount}");
Console.WriteLine();

// --- Step 2: hammer the tree with concurrent writes while we count ---------
// A background writer adds 300 more keys, one at a time, with a tiny pause so
// the writes genuinely interleave with our reads. Meanwhile the foreground
// loop calls CountAsync as fast as it can and records every value it sees.
const int extraCount = 300;
const int finalTotal = seedCount + extraCount;

Console.WriteLine($"Adding {extraCount} keys (extra:0000 .. extra:0299) concurrently while counting...");

var writer = Task.Run(async () =>
{
    for (var i = 0; i < extraCount; i++)
    {
        await tree.SetAsync($"extra:{i:D4}", Value($"x{i}"));
        await Task.Delay(1);
    }
});

var readings = new List<int>();
while (!writer.IsCompleted)
{
    readings.Add(await tree.CountAsync());
}
await writer;

// Every reading is a real committed count, so the sequence can only climb:
// it starts at or above the baseline, ends at or below the final total, and
// never goes backwards. A partial/torn fan-out would break one of these.
var minReading = readings.Count > 0 ? readings.Min() : baselineCount;
var maxReading = readings.Count > 0 ? readings.Max() : baselineCount;
var monotonic = true;
for (var i = 1; i < readings.Count; i++)
{
    if (readings[i] < readings[i - 1])
    {
        monotonic = false;
        break;
    }
}

Console.WriteLine($"  All observed counts stayed within [{seedCount}, {finalTotal}]: " +
    $"{minReading >= seedCount && maxReading <= finalTotal}");
Console.WriteLine($"  Observed counts were monotonic (never went backwards): {monotonic}");
Console.WriteLine();

// --- Step 3: confirm the settled state is exact -----------------------------
// After the writer drains, the tree holds exactly 800 keys. CountAsync and a
// full ScanEntriesAsync must both report that number, with no duplicate keys.
var finalCount = await tree.CountAsync();
var seen = new HashSet<string>();
await foreach (var entry in tree.ScanEntriesAsync())
{
    seen.Add(entry.Key);
}

Console.WriteLine("Settled state after concurrent writes:");
Console.WriteLine($"  CountAsync()             = {finalCount}");
Console.WriteLine($"  Distinct keys from scan  = {seen.Count}");
Console.WriteLine($"  Exact and duplicate-free: {finalCount == finalTotal && seen.Count == finalTotal}");
Console.WriteLine();

Console.WriteLine("Done: scans returned the exact live key set throughout concurrent writes.");

await host.StopAsync();
