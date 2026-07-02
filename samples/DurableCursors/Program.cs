using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// =============================================================================
// DurableCursors
// -----------------------------------------------------------------------------
// Demonstrates a server-checkpointed iterator: OpenEntryCursorAsync returns an
// opaque cursor ID whose paging position is persisted in Orleans storage after
// every page. Any client that knows the ID can resume exactly where the last
// one stopped - no re-scanning, no duplicates, no gaps - even across a client
// restart, silo failover, or topology change.
//
// We make the durability point observable WITHOUT tearing down the whole
// process: after reading the first page we throw away every local variable
// EXCEPT the opaque cursor ID string (imagine it was written to a database or
// a queue), then "reconnect" as a brand new client that knows only that ID and
// resume paging from the persisted checkpoint.
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
var tree = grainFactory.GetGrain<ILattice>("durable-cursors");

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);

// --- Step 1: seed 25 ordered rows ------------------------------------------
const int rowCount = 25;
const int pageSize = 10;
Console.WriteLine($"Seeding {rowCount} keys (row:00 .. row:24)...");
for (var i = 0; i < rowCount; i++)
{
    await tree.SetAsync($"row:{i:D2}", Value($"payload-{i}"));
}
Console.WriteLine();

// A durable cursor ID is just a string. This is the ONLY state a resuming
// client needs; everything else lives server-side in the cursor grain.
var cursorId = await tree.OpenEntryCursorAsync();
Console.WriteLine($"Opened durable entry cursor: {cursorId}");
Console.WriteLine();

var collected = new List<string>();

// --- Step 2: first client reads page 1, then "goes away" -------------------
Console.WriteLine("[Client A] reading page 1...");
var page1 = await tree.NextEntriesAsync(cursorId, pageSize);
foreach (var kv in page1.Entries)
{
    collected.Add(kv.Key);
}
Console.WriteLine($"  got {page1.Entries.Count} keys: {page1.Entries[0].Key} .. {page1.Entries[^1].Key}");
Console.WriteLine($"  HasMore = {page1.HasMore}");
Console.WriteLine();

// Simulate the client process/connection dying. We persist ONLY the cursor ID
// (as if to durable storage) and deliberately drop every other local handle.
var resumeToken = cursorId;
Console.WriteLine("[Client A] crashed / disconnected. The only thing that survives");
Console.WriteLine($"           is the persisted cursor ID: {resumeToken}");
Console.WriteLine("           The server-side checkpoint remembers the last yielded key.");
Console.WriteLine();

// --- Step 3: a brand new client resumes from the checkpoint -----------------
// This client knows nothing except the token. It re-resolves the same tree
// grain and keeps calling NextEntriesAsync with the token. The cursor grain
// reads its persisted LastYieldedKey and continues strictly after it.
Console.WriteLine("[Client B] reconnecting with only the resume token...");
var resumedTree = grainFactory.GetGrain<ILattice>("durable-cursors");

var pageNumber = 2;
while (true)
{
    var page = await resumedTree.NextEntriesAsync(resumeToken, pageSize);
    if (page.Entries.Count > 0)
    {
        Console.WriteLine($"  [Client B] page {pageNumber}: {page.Entries.Count} keys " +
            $"({page.Entries[0].Key} .. {page.Entries[^1].Key})");
    }
    foreach (var kv in page.Entries)
    {
        collected.Add(kv.Key);
    }
    pageNumber++;
    if (!page.HasMore)
    {
        break;
    }
}
Console.WriteLine();

// The cursor grain deactivates and clears its checkpoint on close.
await resumedTree.CloseCursorAsync(resumeToken);

// --- Step 4: verify the resumed scan was seamless ---------------------------
var distinct = new HashSet<string>(collected);
var expected = Enumerable.Range(0, rowCount).Select(i => $"row:{i:D2}").ToList();
var noDuplicates = distinct.Count == collected.Count;
var complete = distinct.SetEquals(expected);
var ordered = collected.SequenceEqual(collected.OrderBy(k => k, StringComparer.Ordinal));

Console.WriteLine("Resumed scan results (page 1 from Client A, rest from Client B):");
Console.WriteLine($"  total keys yielded  = {collected.Count}");
Console.WriteLine($"  no duplicates       = {noDuplicates}");
Console.WriteLine($"  every key exactly once and in order = {complete && ordered && noDuplicates}");
Console.WriteLine();
Console.WriteLine("Done: the cursor resumed from its persisted checkpoint after a client restart.");

await host.StopAsync();
