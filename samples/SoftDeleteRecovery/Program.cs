using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// =============================================================================
// SoftDeleteRecovery
// -----------------------------------------------------------------------------
// Demonstrates that DeleteTreeAsync is a SOFT delete: the tree is immediately
// made inaccessible (reads and writes throw) but its data is retained for a
// configurable grace window. During that window RecoverTreeAsync brings the
// tree - and all its data - back. Contrast that with PurgeTreeAsync, which
// destroys the data immediately and irreversibly.
//
// To keep the demo fast we set a long soft-delete window (recovery must happen
// BEFORE purge) and drive purge explicitly with PurgeTreeAsync at the end.
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
        // Keep the soft-delete window comfortably long so the reminder-driven
        // purge never fires during this short demo. Recovery is only possible
        // while the tree is inside this window.
        silo.ConfigureLattice(o => o.SoftDeleteDuration = TimeSpan.FromHours(72));
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>("customers");

static byte[] Value(string s) => Encoding.UTF8.GetBytes(s);
static string Text(byte[] b) => Encoding.UTF8.GetString(b);

// Try a read and report whether the tree is currently accessible.
static async Task<bool> IsAccessibleAsync(ILattice tree)
{
    try
    {
        await tree.GetAsync("customer:0");
        return true;
    }
    catch (InvalidOperationException)
    {
        return false;
    }
}

// --- Step 1: populate the tree ---------------------------------------------
const int keyCount = 5;
Console.WriteLine($"Seeding tree 'customers' with {keyCount} keys...");
for (var i = 0; i < keyCount; i++)
{
    await tree.SetAsync($"customer:{i}", Value($"name-{i}"));
}
Console.WriteLine($"  accessible = {await IsAccessibleAsync(tree)}, count = {await tree.CountAsync()}");
Console.WriteLine();

// --- Step 2: soft-delete ----------------------------------------------------
// The tree is marked deleted on every shard. Reads/writes now throw
// InvalidOperationException immediately, but the data still exists in storage.
Console.WriteLine("Soft-deleting the tree (DeleteTreeAsync)...");
await tree.DeleteTreeAsync();

var accessibleAfterDelete = await IsAccessibleAsync(tree);
Console.WriteLine($"  accessible after delete = {accessibleAfterDelete} (expected False)");
Console.Write("  attempting a read... ");
try
{
    await tree.GetAsync("customer:0");
    Console.WriteLine("unexpectedly succeeded");
}
catch (InvalidOperationException ex)
{
    Console.WriteLine($"blocked: {ex.Message}");
}
Console.WriteLine();

// --- Step 3: recover within the window --------------------------------------
// Because we are still inside the soft-delete window (nothing has been purged),
// RecoverTreeAsync restores the tree with all its original data intact.
Console.WriteLine("Recovering the tree (RecoverTreeAsync)...");
await tree.RecoverTreeAsync();

var accessibleAfterRecover = await IsAccessibleAsync(tree);
var recoveredCount = await tree.CountAsync();
var sample = await tree.GetAsync("customer:3");
Console.WriteLine($"  accessible after recover = {accessibleAfterRecover} (expected True)");
Console.WriteLine($"  count after recover      = {recoveredCount} (expected {keyCount})");
Console.WriteLine($"  customer:3               = \"{Text(sample!)}\" (data intact)");
Console.WriteLine();

// --- Step 4: contrast with purge (permanent) --------------------------------
// PurgeTreeAsync bypasses the grace window and destroys the data now. After a
// delete+purge, recovery is impossible - the tree is gone for good.
Console.WriteLine("Now permanently destroying the tree (DeleteTreeAsync + PurgeTreeAsync)...");
await tree.DeleteTreeAsync();
await tree.PurgeTreeAsync();

Console.Write("  attempting RecoverTreeAsync after purge... ");
try
{
    await tree.RecoverTreeAsync();
    Console.WriteLine("unexpectedly succeeded");
}
catch (InvalidOperationException ex)
{
    Console.WriteLine($"refused: {ex.Message}");
}
Console.WriteLine();

Console.WriteLine("Done: soft-delete blocked access and was reversible; purge was permanent.");

await host.StopAsync();
