using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Samples.MaterialisedViews;
using Orleans.Lattice.Views;

// MaterialisedViews sample
// ========================
// A materialised view is an asynchronous, eventually-consistent projection of a
// source tree, maintained by tailing that tree's write-ahead log. This sample
// declares TWO views over a "people" tree and shows each converging to reflect
// source writes:
//   1. A filter / re-project view "adults" - keeps only people with Age >= 18.
//   2. An aggregation view "age-sum-by-city" - sums Age grouped by City.
//
// The views are declared at STARTUP via AddLatticeViews(...), so their
// maintainers come online with the host - before any writes - and every source
// write then flows through the projection as it lands.

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
        silo.AddLatticeViews(views =>
        {
            // Filter / re-project: keep only source people with Age >= 18.
            views.AddView(
                viewName: "adults",
                sourceTreeId: "people",
                projection: new PredicateLatticeViewProjection(
                    LatticePredicateTranslator.Translate<User>(u => u.Age >= 18)));

            // Aggregation: sum Age grouped by City, one reduced value per city.
            views.AddAggregationView(
                viewName: "age-sum-by-city",
                sourceTreeId: "people",
                projection: AggregationLatticeViewProjection.Create<User>(
                    AggregationKind.Sum,
                    groupKeySelector: u => u.City,
                    selectorVersion: "sum-age-by-city-v1",
                    valueSelector: u => u.Age));
        });
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grains = host.Services.GetRequiredService<IGrainFactory>();
var factory = host.Services.GetRequiredService<ILatticeViewFactory>();

var people = grains.GetGrain<ILattice>("people");
var serializer = JsonLatticeSerializer<User>.Default;

// Resolve read handles for the startup-declared views.
var adults = await factory.GetAsync("adults") ?? throw new InvalidOperationException("adults view missing");
var ageByCity = await factory.GetAsync("age-sum-by-city") ?? throw new InvalidOperationException("aggregation view missing");

async Task PutAsync(User user) => await people.SetAsync(user.Name, serializer.Serialize(user));

// Seed the source tree.
User[] seed =
[
    new("Alice", 34, "London"),
    new("Bob", 12, "London"),
    new("Carol", 27, "Paris"),
    new("Dan", 9, "Paris"),
    new("Eve", 41, "London"),
];

Console.WriteLine("Writing source people:");
foreach (var user in seed)
{
    await PutAsync(user);
    Console.WriteLine($"  {user.Name} (age {user.Age}, {user.City})");
}
Console.WriteLine();

// Views are eventually consistent: wait (bounded) for both maintainers to apply
// every source write so the output is deterministic.
await adults.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
await ageByCity.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

async Task DumpAsync(string label)
{
    Console.WriteLine(label);

    Console.WriteLine($"  'adults' view ({await adults.CountAsync()} keys, lag {await adults.GetLagAsync()}):");
    await foreach (var name in adults.KeysAsync())
    {
        var user = await adults.GetAsync<User>(name);
        Console.WriteLine($"    {user!.Name} (age {user.Age})");
    }

    Console.WriteLine("  'age-sum-by-city' aggregate:");
    foreach (var city in new[] { "London", "Paris" })
    {
        var total = await ageByCity.GetAggregateDoubleAsync(city) ?? 0;
        Console.WriteLine($"    {city}: sum(age) = {total}");
    }

    Console.WriteLine();
}

await DumpAsync("After initial seed - views converged to source:");

// Mutate the source: add an adult and promote a child past 18. Both views
// converge to reflect the new source state without any direct writes to them.
Console.WriteLine("Mutating source: add Frank(52, Paris); Bob turns 18 (London).");
await PutAsync(new User("Frank", 52, "Paris"));
await PutAsync(new User("Bob", 18, "London"));
Console.WriteLine();

await adults.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
await ageByCity.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

await DumpAsync("After mutation - views re-converged:");

await host.StopAsync();
