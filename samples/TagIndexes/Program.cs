using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// TagIndexes
// ---------------------------------------------------------------------------
// A tag index associates string tags with the keys of a tree and lets you query
// keys back by tag. It is built entirely on the public ILattice surface (the
// membership rows live in a sibling `tag-{indexName}` tree) - no extra grain or
// storage provider to register.
//
// This sample tags a handful of catalogue items by colour and shape, then runs
// the two query directions:
//   * WithAllTags  -> intersection: keys carrying EVERY listed tag.
//   * WithAnyTags  -> de-duplicated union: keys carrying ANY listed tag.
//
// See docs/lattice/api.md#tag-indexes.
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

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var catalogue = grainFactory.GetGrain<ILattice>("catalogue");

// The subject tree must be registered (have at least one write) before its keys
// can be tagged, so seed the item values first.
var items = new (string Key, string[] Tags)[]
{
    ("item:1", ["red", "round"]),
    ("item:2", ["red", "square"]),
    ("item:3", ["blue", "round"]),
    ("item:4", ["green", "square"]),
};

Console.WriteLine("== Seeding catalogue items ==");
foreach (var (key, _) in items)
{
    await catalogue.SetAsync(key, Encoding.UTF8.GetBytes($"payload for {key}"));
    Console.WriteLine($"  wrote {key}");
}
Console.WriteLine();

// Open a tag index over the catalogue tree. The factory is registered as a
// singleton by AddLattice; resolve it from the host's service provider.
var tagIndexFactory = host.Services.GetRequiredService<ILatticeTagIndexFactory>();
var byFacet = tagIndexFactory.Create(catalogue, "by-facet");

Console.WriteLine("== Associating tags with keys ==");
foreach (var (key, tags) in items)
{
    await byFacet.Key(key).AddAsync(tags);
    Console.WriteLine($"  {key} += [{string.Join(", ", tags)}]");
}
Console.WriteLine();

// WithAllTags: intersection. Only keys carrying BOTH "red" AND "round".
Console.WriteLine("== WithAllTags(\"red\", \"round\") - intersection ==");
await foreach (var key in byFacet.WithAllTags("red", "round"))
{
    Console.WriteLine($"  {key}");
}
var allCount = await byFacet.WithAllTags("red", "round").CountAsync();
Console.WriteLine($"  count = {allCount}  (expected item:1 only)");
Console.WriteLine();

// WithAnyTags: union. Any key carrying "red" OR "blue", de-duplicated.
Console.WriteLine("== WithAnyTags(\"red\", \"blue\") - union ==");
await foreach (var key in byFacet.WithAnyTags("red", "blue"))
{
    Console.WriteLine($"  {key}");
}
var anyCount = await byFacet.WithAnyTags("red", "blue").CountAsync();
Console.WriteLine($"  count = {anyCount}  (expected item:1, item:2, item:3)");
Console.WriteLine();

Console.WriteLine("Done. WithAllTags narrows (AND); WithAnyTags widens (OR).");

await host.StopAsync();
