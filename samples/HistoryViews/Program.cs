using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Views;

// HistoryViews sample
// ===================
// Demonstrates a DURABLE per-key history view: an opt-in, append-only
// materialised view that records every revision of every key in a source tree.
// Unlike the best-effort WAL-window fallback (see the ChangeHistory sample), the
// history view survives source WAL garbage collection and is bounded only by its
// configured retention age, so ScanEntryHistoryAsync reports Source == View and
// never truncates the timeline below.
//
// Two pieces of setup make this work:
//   1. AddLatticeViews() on the silo (the view catalog + maintainer).
//   2. A runtime view created via ILatticeViewFactory using
//      LatticeHistoryView.Definition, plus SetHistoryRetentionAsync to keep the
//      LWW value bytes verbatim (FullValue) so we can read old values back.

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
        // The view subsystem: catalog, factory, and maintainer. Required for a
        // durable history view.
        silo.AddLatticeViews();
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grains = host.Services.GetRequiredService<IGrainFactory>();
var factory = host.Services.GetRequiredService<ILatticeViewFactory>();

var orders = grains.GetGrain<ILattice>("orders");
const string key = "order-42";

// Keep full value bytes for every revision (no age bound) so the durable
// timeline can serve point-in-time values directly.
// Pass null for the window to clear any age bound (revisions never expire);
// the setter rejects TimeSpan.Zero - use null to mean "no age bound".
await orders.SetHistoryRetentionAsync(
    HistoryRetentionMode.FullValue,
    window: null,
    CancellationToken.None);
var retention = await orders.GetHistoryRetentionAsync();
Console.WriteLine($"History retention for 'orders': mode={retention.Mode}, window={retention.Window}");

// Enable history: a runtime view named "orders-history" tailing "orders".
// History is forward-only, so create it BEFORE writing the revisions we want kept.
var history = factory.Create(
    orders,
    "orders-history",
    LatticeHistoryView.Definition("orders-history", host.Services));
Console.WriteLine("Durable history view 'orders-history' created (forward-only).");
Console.WriteLine();

// Write a sequence of values to ONE key. Each Set is a durable revision.
string[] lifecycle = ["placed", "paid", "packed", "shipped", "delivered"];
Console.WriteLine($"Writing {lifecycle.Length} successive revisions to key '{key}':");
foreach (var status in lifecycle)
{
    await orders.SetAsync(key, Encoding.UTF8.GetBytes(status));
    Console.WriteLine($"  set '{key}' = '{status}'");
}
Console.WriteLine();

// The view is eventually consistent: wait (bounded) for the maintainer to apply
// every source write so the sample output is deterministic.
await history.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
Console.WriteLine($"View apply lag after catch-up: {await history.GetLagAsync()}");
Console.WriteLine();

// A plain read collapses to the latest value - the older revisions are gone from
// the live tree. The durable history view is what preserves them.
var current = await orders.GetAsync(key);
Console.WriteLine($"Plain GetAsync('{key}') -> '{Encoding.UTF8.GetString(current!)}' (latest only)");
Console.WriteLine();

// Read the durable timeline. FullValue retention means each revision carries its
// value bytes in ValuePreview, so we can print the historical value verbatim.
Console.WriteLine($"ScanEntryHistoryAsync('{key}') - the durable revision timeline:");
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
        var value = revision.ValuePreview is null
            ? "<metadata-only>"
            : Encoding.UTF8.GetString(revision.ValuePreview);
        Console.WriteLine(
            $"  #{revisionNumber} hlc={revision.Hlc} kind={revision.Kind} " +
            $"value='{value}' (shape={revision.RetentionShape})");
    }

    continuation = page.Continuation;
}
while (continuation is not null);

Console.WriteLine();
Console.WriteLine($"Source={page.Source} (View = durable history view, survives WAL GC)");
Console.WriteLine($"Truncated={page.Truncated} (always false on the View path - bounded only by retention age)");
Console.WriteLine($"Total durable revisions read: {revisionNumber}");

await host.StopAsync();
