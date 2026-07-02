using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ChangeHistory sample
// ====================
// Demonstrates ILattice.ScanEntryHistoryAsync: the per-key revision timeline.
// For ANY key you can ask "how did this value get here?" and read back the
// ordered list of revisions that produced the current state.
//
// This sample uses the zero-setup WAL-window fallback: when a tree has NOT
// opted into a durable history view, ScanEntryHistoryAsync still serves the
// surviving revisions from the source tree's retained write-ahead-log window.
// (Enable a durable, retention-bounded timeline with a history view - see the
// sibling HistoryViews sample.)

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        // Silence Orleans' own logging so the sample output stays clean.
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
var orders = grains.GetGrain<ILattice>("orders");

const string key = "order-42";

// Write a sequence of values to ONE key. Each Set is a distinct revision in the
// key's timeline, stamped with a monotonically advancing hybrid logical clock.
string[] lifecycle =
[
    "placed",
    "paid",
    "packed",
    "shipped",
    "delivered",
];

Console.WriteLine($"Writing {lifecycle.Length} successive revisions to key '{key}':");
foreach (var status in lifecycle)
{
    await orders.SetAsync(key, System.Text.Encoding.UTF8.GetBytes(status));
    Console.WriteLine($"  set '{key}' = '{status}'");
}
Console.WriteLine();

// A plain read only ever sees the LATEST value - the history is invisible to it.
var current = await orders.GetAsync(key);
Console.WriteLine($"Plain GetAsync('{key}') -> '{System.Text.Encoding.UTF8.GetString(current!)}' (latest only)");
Console.WriteLine();

// ScanEntryHistoryAsync returns the whole timeline, oldest first, paged with a
// continuation token. We pass fromHlc/toHlc = null to read the full window.
Console.WriteLine($"ScanEntryHistoryAsync('{key}') - the revision timeline:");
var revisionNumber = 0;
string? continuation = null;
EntryHistoryPage page;
do
{
    page = await orders.ScanEntryHistoryAsync(
        key,
        fromHlc: null,
        toHlc: null,
        limit: 100,
        continuation: continuation,
        CancellationToken.None);

    foreach (var revision in page.Revisions)
    {
        revisionNumber++;
        // MetadataOnly-style fallback rows still carry a content hash + length so a
        // consumer can detect change even without the raw bytes.
        Console.WriteLine(
            $"  #{revisionNumber} hlc={revision.Hlc} kind={revision.Kind} " +
            $"valueLen={revision.ValueLength} valueHash=0x{revision.ValueHash:x8}");
    }

    continuation = page.Continuation;
}
while (continuation is not null);

Console.WriteLine();
Console.WriteLine($"Source={page.Source} (WalWindow = best-effort fallback, no history view enabled)");
Console.WriteLine($"Truncated={page.Truncated} (true would mean older revisions were already trimmed by WAL GC)");
Console.WriteLine($"Total revisions read: {revisionNumber}");

await host.StopAsync();
