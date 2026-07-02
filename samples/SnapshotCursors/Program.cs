using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// =============================================================================
// SnapshotCursors
// -----------------------------------------------------------------------------
// Demonstrates strict snapshot isolation: OpenSnapshotEntryCursorAsync freezes
// the tree state at open time. Every page the cursor returns reflects that
// captured instant, and NO concurrent write - foreground SetAsync/DeleteAsync,
// atomic saga, or range delete - is ever visible to the cursor for the rest of
// its lifetime.
//
// This is the difference from a live cursor: a report or export that must
// reflect a single point in time can page slowly while the tree keeps changing,
// and still see a perfectly stable view.
//
// We prove it by writing NEW and MODIFIED keys mid-iteration and showing the
// snapshot cursor does not observe either, while a fresh read does.
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
var tree = grainFactory.GetGrain<ILattice>("snapshot-cursors");

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);
static string Text(byte[] b) => Encoding.UTF8.GetString(b);

// --- Step 1: seed 20 keys, all with the original value "v0" ----------------
const int seedCount = 20;
const int pageSize = 5;
Console.WriteLine($"Seeding {seedCount} keys (k:00 .. k:19), each value = \"v0\"...");
for (var i = 0; i < seedCount; i++)
{
    await tree.SetAsync($"k:{i:D2}", Value("v0"));
}
Console.WriteLine();

// --- Step 2: open a snapshot cursor - this freezes the view ----------------
var cursorId = await tree.OpenSnapshotEntryCursorAsync();
Console.WriteLine($"Opened snapshot entry cursor: {cursorId}");
Console.WriteLine("  The tree state is now frozen for this cursor's lifetime.");
Console.WriteLine();

// Read the first page (k:00 .. k:04).
var page1 = await tree.NextEntriesAsync(cursorId, pageSize);
Console.WriteLine($"Snapshot page 1: {page1.Entries[0].Key} .. {page1.Entries[^1].Key}");
Console.WriteLine();

// --- Step 3: MUTATE the tree while the cursor is mid-iteration --------------
// Add a brand new key and overwrite an existing one that the cursor has not
// paged past yet. A live cursor would see these; a snapshot cursor must not.
Console.WriteLine("Mid-iteration writes (should be INVISIBLE to the snapshot cursor):");
await tree.SetAsync("k:99", Value("brand-new"));
Console.WriteLine("  + added new key   k:99 = \"brand-new\"");
await tree.SetAsync("k:10", Value("MODIFIED"));
Console.WriteLine("  ~ overwrote       k:10 = \"MODIFIED\"");
Console.WriteLine();

// --- Step 4: drain the rest of the snapshot cursor --------------------------
var snapshotEntries = new List<KeyValuePair<string, byte[]>>(page1.Entries);
while (true)
{
    var page = await tree.NextEntriesAsync(cursorId, pageSize);
    snapshotEntries.AddRange(page.Entries);
    if (!page.HasMore)
    {
        break;
    }
}
await tree.CloseCursorAsync(cursorId);

var snapshotKeys = new HashSet<string>(snapshotEntries.Select(e => e.Key));
var k10Snapshot = snapshotEntries.First(e => e.Key == "k:10").Value;

Console.WriteLine("What the snapshot cursor saw:");
Console.WriteLine($"  total entries         = {snapshotEntries.Count} (expected {seedCount})");
Console.WriteLine($"  contains new key k:99 = {snapshotKeys.Contains("k:99")} (expected False)");
Console.WriteLine($"  value of k:10         = \"{Text(k10Snapshot)}\" (expected \"v0\")");
Console.WriteLine();

// --- Step 5: a fresh read DOES see the writes -------------------------------
// Same tree, read live: the mutations are of course present. This contrast is
// the whole point - the snapshot cursor was isolated, live reads are not.
var liveCount = await tree.CountAsync();
var k10Live = await tree.GetAsync("k:10");
Console.WriteLine("What a fresh live read sees (for contrast):");
Console.WriteLine($"  CountAsync()  = {liveCount} (expected {seedCount + 1})");
Console.WriteLine($"  value of k:10 = \"{Text(k10Live!)}\" (expected \"MODIFIED\")");
Console.WriteLine();

var isolated = snapshotEntries.Count == seedCount
    && !snapshotKeys.Contains("k:99")
    && Text(k10Snapshot) == "v0";
Console.WriteLine($"Snapshot isolation held: {isolated}");
Console.WriteLine();
Console.WriteLine("Done: the snapshot cursor never observed writes made after it was opened.");

await host.StopAsync();
