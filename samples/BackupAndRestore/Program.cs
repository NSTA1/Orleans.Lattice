using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Api.Backup;

// =============================================================================
// BackupAndRestore
// -----------------------------------------------------------------------------
// Demonstrates the Orleans.Lattice backup surface end to end against a single
// in-process silo with the default in-cluster sink:
//
//   1. Register a backup scope and a full + incremental schedule.
//   2. Seed a tree with some keys.
//   3. Trigger a FULL backup on demand.
//   4. Mutate the tree (add, change, and delete keys).
//   5. Trigger an INCREMENTAL backup layered on the full base.
//   6. List the catalog.
//   7. Restore the latest backup (base + increment folded) into a FRESH tree.
//   8. Print the restored values and an inventory summary.
//
// The scheduled full / incremental reminders are registered to show the API,
// but the captures themselves are driven synchronously through the scheduler's
// on-demand trigger methods so the sample runs to completion deterministically
// instead of waiting on the one-minute reminder floor.
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
        silo.AddLatticeBackup();
        silo.AddLatticeBackupApi();

        // Enable a daily full + hourly incremental schedule for every scope. The
        // reminders are registered by EnsureScheduleAsync below; this sample then
        // drives the captures by hand so it does not have to wait for them.
        silo.ConfigureLatticeBackupSchedule(schedule =>
        {
            schedule.FullBackupScheduleEnabled = true;
            schedule.FullBackupInterval = TimeSpan.FromDays(1);
            schedule.IncrementalBackupScheduleEnabled = true;
            schedule.IncrementalBackupInterval = TimeSpan.FromHours(1);
        });
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var scheduler = host.Services.GetRequiredService<ILatticeBackupScheduler>();
var catalog = host.Services.GetRequiredService<ILatticeBackupCatalogStore>();
var restore = host.Services.GetRequiredService<ILatticeBackupRestoreService>();

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);
static string Text(byte[] b) => Encoding.UTF8.GetString(b);

const string treeId = "orders";
const string restoredTreeId = "orders-restored";
var scope = BackupScopeSelector.WholeTree(treeId);
var source = grainFactory.GetGrain<ILattice>(treeId);

// --- Step 1: register the scope and its full + incremental schedule ---------
Console.WriteLine("Registering backup scope 'orders' with a full + incremental schedule...");
await scheduler.EnsureScheduleAsync(scope);
Console.WriteLine();

// --- Step 2: seed the source tree -------------------------------------------
const int keyCount = 8;
Console.WriteLine($"Seeding tree 'orders' with {keyCount} keys...");
for (var i = 0; i < keyCount; i++)
{
    await source.SetAsync($"order:{i:D3}", Value($"amount={i * 10}"));
}

Console.WriteLine($"  live count = {await source.CountAsync()}");
Console.WriteLine();

// --- Step 3: trigger a full backup on demand --------------------------------
Console.Write("Triggering a FULL backup...");
var fullBackupId = await scheduler.TriggerFullBackupAsync(scope);
Console.WriteLine($" captured {fullBackupId}.");
Console.WriteLine();

// --- Step 4: mutate the tree ------------------------------------------------
Console.WriteLine("Mutating the tree (change one, add two, delete one)...");
await source.SetAsync("order:003", Value("amount=CHANGED"));
await source.SetAsync("order:100", Value("amount=1000"));
await source.SetAsync("order:101", Value("amount=1010"));
await source.DeleteAsync("order:000");
Console.WriteLine($"  live count = {await source.CountAsync()}");
Console.WriteLine();

// --- Step 5: trigger an incremental backup layered on the full --------------
Console.Write("Triggering an INCREMENTAL backup...");
var incrementalBackupId = await scheduler.TriggerIncrementalBackupAsync(scope);
Console.WriteLine($" captured {incrementalBackupId}.");
Console.WriteLine();

// --- Step 6: list the catalog -----------------------------------------------
Console.WriteLine("Backup catalog:");
long totalBytes = 0;
var backupCount = 0;
await foreach (var manifest in catalog.ListAsync())
{
    var bytes = manifest.ContentDescriptors.Sum(d => d.ByteLength);
    totalBytes += bytes;
    backupCount++;
    Console.WriteLine(
        $"  {manifest.Id}  kind={manifest.Kind,-11} base={manifest.BaseBackupId ?? "<none>",-40} bytes={bytes}");
}

Console.WriteLine();

// --- Step 7: restore the latest backup into a fresh tree --------------------
// Restoring the incremental backup folds its base full backup and the increment
// into a single faithful image, written into a brand-new tree that did not
// exist before.
Console.Write($"Restoring {incrementalBackupId} into fresh tree '{restoredTreeId}'...");
var restoreResult = await restore.RestoreAsync(
    new LatticeRestoreRequest(incrementalBackupId!, targetTreeId: restoredTreeId));
Console.WriteLine($" applied {restoreResult.EntriesApplied} entries over a chain of {restoreResult.ManifestChain.Count}.");
Console.WriteLine();

// --- Step 8: print the restored values and an inventory summary -------------
var restored = grainFactory.GetGrain<ILattice>(restoredTreeId);
Console.WriteLine("Restored values (should match the live tree after mutation):");
foreach (var key in new[] { "order:000", "order:001", "order:003", "order:100", "order:101" })
{
    var live = await source.GetAsync(key);
    var back = await restored.GetAsync(key);
    var liveText = live is null ? "<absent>" : Text(live);
    var backText = back is null ? "<absent>" : Text(back);
    var match = liveText == backText ? "OK" : "MISMATCH";
    Console.WriteLine($"  {key}: live=\"{liveText}\" restored=\"{backText}\" [{match}]");
}

Console.WriteLine();
Console.WriteLine("Inventory summary (from the public catalog store):");
Console.WriteLine($"  backup count       = {backupCount}");
Console.WriteLine($"  total catalog bytes = {totalBytes}");
Console.WriteLine($"  restored live count = {await restored.CountAsync()} (expected {await source.CountAsync()})");
Console.WriteLine();

Console.WriteLine("Done: full + incremental capture round-tripped faithfully into a fresh tree.");

await host.StopAsync();
