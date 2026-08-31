using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.GrainIndex;
using Orleans.Lattice.Samples.GrainIndex;

// GrainIndex sample
// =================
// A grain index tracks a grain's typed state in a lattice tree so you can ask
// "which User grains are 18 or over?" without hand-maintaining a secondary
// index and without activating every grain to find out.
//
// This sample:
//   1. Declares an index over IUserGrain's UserState, projecting Age and Country.
//   2. Writes a handful of users, each of which enrols itself on its write path.
//   3. Runs typed predicate queries against the index.
//   4. Shows a conjunction (Age >= 18 && Country == "UK"), which the planner
//      turns into two range scans whose grain keys are intersected.

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

        // Declare the index. Only the properties named here are projected, so
        // the write amplification of an index is always a deliberate choice.
        silo.AddGrainIndex<IUserGrain, UserState>(index => index
            .WithName("users")
            .Include(u => u.Age)
            .Include(u => u.Country));
    })
    .Build();

await host.StartAsync();

var grains = host.Services.GetRequiredService<IGrainFactory>();
var indexes = host.Services.GetRequiredService<IGrainIndexProvider>();

// 1. Write some users. Each WriteStateAsync re-projects that grain's entries.
var people = new (string Id, int Age, string Country)[]
{
    ("alice", 34, "UK"),
    ("bob", 17, "UK"),
    ("carla", 22, "IE"),
    ("dan", 61, "UK"),
    ("erin", 15, "IE"),
};

foreach (var (id, age, country) in people)
{
    await grains.GetGrain<IUserGrain>(id).SetProfileAsync(age, country);
}

Console.WriteLine($"Wrote {people.Length} users.");
Console.WriteLine();

var index = indexes.GetIndex<IUserGrain, UserState>("users");

// 2. A single-property comparison becomes one contiguous range scan over the
//    order-preserving key encoding - not a full scan plus a filter.
Console.WriteLine("Adults (Age >= 18):");
await foreach (var key in index.Where(u => u.Age >= 18).ToKeysAsync())
{
    Console.WriteLine($"  {key}");
}

Console.WriteLine();

// 3. Equality on a string property works the same way.
Console.WriteLine("Users in the UK:");
await foreach (var key in index.Where(u => u.Country == "UK").ToKeysAsync())
{
    Console.WriteLine($"  {key}");
}

Console.WriteLine();

// 4. A conjunction over two properties cannot be one predicate, because an
//    index entry carries exactly one property. The planner issues one range
//    scan per property and intersects the resulting grain keys.
Console.WriteLine("UK adults (Age >= 18 && Country == \"UK\"):");
await foreach (var grain in index
    .Where(u => u.Age >= 18 && u.Country == "UK")
    .ToGrainsAsync())
{
    // The index is eventually consistent with respect to grain state, so
    // confirm against the grain when the answer must be authoritative.
    var age = await grain.GetAgeAsync();
    Console.WriteLine($"  {grain.GetPrimaryKeyString()} (age {age})");
}

Console.WriteLine();

// 5. A disjunction unions its branches and de-duplicates, so a grain matching
//    both branches is yielded once.
var teenagersOrSeniors = await index
    .Where(u => u.Age < 18 || u.Age >= 60)
    .ToKeyListAsync();

Console.WriteLine($"Under 18 or 60+: {string.Join(", ", teenagersOrSeniors)}");

Console.WriteLine();
Console.WriteLine("Done.");

await host.StopAsync();
