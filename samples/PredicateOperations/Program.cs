using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// PredicateOperations - server-side predicate push-down.
//
// A typed read/scan can carry an ordinary C# Expression<Func<T,bool>>. The
// lambda is compiled to a small serializable IR on the client and evaluated ON
// THE LEAF GRAIN that owns each key. Only matching keys (or values) travel back
// across the wire - non-matching values are dropped at the source, so the client
// never pays to ship or deserialize data it would immediately discard.
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

Console.WriteLine("== PredicateOperations sample ==");
Console.WriteLine();

// Seed a small population of users. Ages straddle the 18 boundary so the
// predicates below have something to filter.
var users = grainFactory.GetGrain<ILattice>("users");
var population = new (string Key, User Value)[]
{
    ("user:1", new User("Alice", 31)),
    ("user:2", new User("Bob", 12)),
    ("user:3", new User("Carol", 18)),
    ("user:4", new User("Dave", 9)),
    ("user:5", new User("Erin", 47)),
};
foreach (var (key, value) in population)
    await users.SetAsync(key, value);

Console.WriteLine("Seeded 5 users (ages 31, 12, 18, 9, 47).");
Console.WriteLine();

// --- 1. GetManyAsync with a predicate --------------------------------------
// We ask for all five keys but push down "Age >= 18". Underage users are
// filtered on their owning leaf and are simply absent from the result - their
// values never cross the wire.
var keys = new List<string> { "user:1", "user:2", "user:3", "user:4", "user:5" };
var adults = await users.GetManyAsync<User>(keys, u => u.Age >= 18);

Console.WriteLine("1) GetManyAsync(keys, u => u.Age >= 18):");
foreach (var (key, user) in adults.OrderBy(kv => kv.Key))
    Console.WriteLine($"   {key} -> {user.Name} ({user.Age})");
Console.WriteLine($"   -> {adults.Count} of 5 keys matched; the rest were dropped server-side.");
Console.WriteLine();

// --- 2. ScanKeysAsync with a predicate (keys only) --------------------------
// A key-only scan evaluates the predicate on the leaf but ships back ONLY the
// keys - no values travel at all. Here: names starting with a vowel among
// adults.
Console.WriteLine("2) ScanKeysAsync(u => u.Age >= 18 && u.Name.StartsWith(\"A\")): keys only");
await foreach (var key in users.ScanKeysAsync<User>(
    u => u.Age >= 18 && u.Name.StartsWith("A")))
{
    Console.WriteLine($"   matched key: {key}");
}
Console.WriteLine("   -> only matching keys crossed the wire; no User values were shipped.");
Console.WriteLine();

// --- 3. ScanEntriesAsync with a predicate (matching values) -----------------
// An entry scan ships back only the values that match, so a large scan that
// selects a small subset stays cheap.
Console.WriteLine("3) ScanEntriesAsync(u => u.Age < 18): only matching entries");
await foreach (var entry in users.ScanEntriesAsync<User>(u => u.Age < 18))
    Console.WriteLine($"   {entry.Key} -> {entry.Value.Name} ({entry.Value.Age})");
Console.WriteLine("   -> only the minors' values were materialized client-side.");
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();
