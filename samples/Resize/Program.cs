using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// Resize - change a live tree's structural sizing with ResizeAsync.
//
// MaxLeafKeys / MaxInternalChildren control the tree's fan-out. They are pinned
// per tree in the registry, and the only supported way to change them on a tree
// that already holds data is ResizeAsync, which runs ONLINE: it drains the
// source into a freshly-sized destination tree (shadow-forwarding live writes)
// and atomically swaps the alias. Reads and writes stay available throughout and
// every entry is preserved verbatim. This sample populates a tree past a single
// leaf, resizes it, and confirms the data survived.
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

Console.WriteLine("== Resize sample ==");
Console.WriteLine();

// Populate the tree with more entries than the default 128 keys-per-leaf so the
// tree is genuinely multi-leaf and the resize has real structure to rebuild.
const int count = 500;
var tree = grainFactory.GetGrain<ILattice>("catalog");
var entries = new List<KeyValuePair<string, byte[]>>(count);
for (var i = 0; i < count; i++)
    entries.Add(new KeyValuePair<string, byte[]>($"item:{i:D4}", Encoding.UTF8.GetBytes($"value-{i}")));
await tree.SetManyAsync(entries);

Console.WriteLine($"Populated 'catalog' with {await tree.CountAsync()} entries (default MaxLeafKeys=128).");
Console.WriteLine();

// Kick off the online resize to a wider leaf capacity. ResizeAsync returns once
// the pipeline is initiated; we poll IsResizeCompleteAsync until the alias swap
// and cleanup have finished.
Console.WriteLine("Calling ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64)...");
var sw = Stopwatch.StartNew();
await tree.ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64);

var deadline = TimeSpan.FromSeconds(60);
while (!await tree.IsResizeCompleteAsync())
{
    if (sw.Elapsed > deadline)
        throw new TimeoutException("Resize did not complete within the expected window.");
    await Task.Delay(200);
}
sw.Stop();
Console.WriteLine($"Resize completed in {sw.Elapsed.TotalSeconds:F1}s.");
Console.WriteLine();

// The tree ID is unchanged and every entry survived the re-paginate + swap.
Console.WriteLine($"CountAsync after resize -> {await tree.CountAsync()} (unchanged).");
var probe = await tree.GetAsync("item:0250");
Console.WriteLine($"item:0250 = {Encoding.UTF8.GetString(probe!)}");

// Writes keep working against the same logical tree after the swap.
await tree.SetAsync("item:new", Encoding.UTF8.GetBytes("post-resize"));
var newProbe = await tree.GetAsync("item:new");
Console.WriteLine($"item:new  = {Encoding.UTF8.GetString(newProbe!)} (written after the resize)");
Console.WriteLine("-> same tree, wider leaves, data intact and still writable.");

Console.WriteLine();
Console.WriteLine("Done.");
await host.StopAsync();
