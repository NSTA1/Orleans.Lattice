using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// BulkLoading - seeding an EMPTY tree cheaply, two ways.
//
// BulkLoadAsync builds the finished tree shape up front and writes each leaf
// once, instead of inserting key-by-key and splitting nodes as it grows. It is
// a one-shot initial-import primitive: every shard must be empty when called.
// This sample shows both entry points:
//   1. One-shot: hand the whole dataset to ILattice.BulkLoadAsync.
//   2. Streaming: feed an IAsyncEnumerable to the LatticeExtensions overload for
//      datasets too large to hold in memory (flushed in fixed-size chunks).
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
    })
    .Build();

await host.StartAsync();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

Console.WriteLine("== BulkLoading sample ==");
Console.WriteLine();

// --- 1. One-shot bulk load --------------------------------------------------
// The whole dataset is materialized in memory and handed over in a single call.
// BulkLoadAsync sorts it internally (input need NOT be pre-sorted), packs each
// leaf to capacity, and commits each shard once.
const int oneShotCount = 5_000;
var dataset = new List<KeyValuePair<string, byte[]>>(oneShotCount);
for (var i = 0; i < oneShotCount; i++)
{
    // Deliberately not pre-sorted order does not matter for the one-shot path.
    var key = $"product:{i:D6}";
    dataset.Add(new KeyValuePair<string, byte[]>(key, Encoding.UTF8.GetBytes($"item-{i}")));
}

var oneShot = grainFactory.GetGrain<ILattice>("products-oneshot");
Console.WriteLine($"1) One-shot BulkLoadAsync of {oneShotCount} entries into an empty tree...");
await oneShot.BulkLoadAsync(dataset);
Console.WriteLine($"   CountAsync -> {await oneShot.CountAsync()}");
var probe = await oneShot.GetAsync("product:002500");
Console.WriteLine($"   product:002500 = {Encoding.UTF8.GetString(probe!)}");
Console.WriteLine("   -> the whole dataset landed in one shot.");
Console.WriteLine();

// --- 2. Streaming bulk load -------------------------------------------------
// The streaming overload never holds the whole dataset in memory: it pulls from
// an IAsyncEnumerable and flushes fixed-size chunks per shard. Entries must
// arrive in ascending key order because each chunk is appended to the right edge
// of the tree.
const int streamCount = 20_000;
var stream = grainFactory.GetGrain<ILattice>("products-stream");
Console.WriteLine($"2) Streaming BulkLoadAsync of {streamCount} entries (chunkSize 4000)...");
await stream.BulkLoadAsync(ReadInKeyOrder(streamCount), grainFactory, chunkSize: 4_000);
Console.WriteLine($"   CountAsync -> {await stream.CountAsync()}");
var streamProbe = await stream.GetAsync("k:00012345");
Console.WriteLine($"   k:00012345 = {Encoding.UTF8.GetString(streamProbe!)}");
Console.WriteLine("   -> ingested incrementally without buffering the whole set.");
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();

// Yields entries in ascending key order, one at a time, as a real streaming
// source would (e.g. reading a sorted file or a query cursor).
static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ReadInKeyOrder(int count)
{
    for (var i = 0; i < count; i++)
    {
        yield return new KeyValuePair<string, byte[]>(
            $"k:{i:D8}", Encoding.UTF8.GetBytes($"v{i}"));
    }

    await Task.CompletedTask;
}
