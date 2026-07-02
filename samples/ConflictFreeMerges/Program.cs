using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// ConflictFreeMerges
// ---------------------------------------------------------------------------
// This sample demonstrates the single idea at the heart of Orleans.Lattice:
// its state types are CRDTs, so two writers that mutate the same logical value
// concurrently - without any coordination - always converge to the SAME final
// state, regardless of the order their updates are merged in.
//
// We show this at the primitive level (the honest, deterministic core) using
// the public PnCounter and OrSet CRDTs, then persist a converged value into a
// real Lattice tree so you can see it round-trip through the store. See
// docs/lattice/state-primitives.md for the merge algebra used here.
// ---------------------------------------------------------------------------

// A single-silo in-process Orleans cluster, wired exactly like HelloWorld.
// Orleans logging is silenced so the narration below is the only output.
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

// The two replica identities. In a real deployment these would be two silos or
// two clusters; here they are just two string ids feeding the CRDT dots.
const string West = "site-west";
const string East = "site-east";

// -- Part 1: PN-Counter --------------------------------------------------
// A PnCounter tracks a per-replica increment/decrement history. Two replicas
// each apply local deltas while "partitioned" (they cannot see each other),
// then reconcile. Because merge is pointwise-max per replica, the reconciled
// value is identical no matter which side merges first.
Console.WriteLine("== PN-Counter: two writers increment the same counter concurrently ==");

var west = new PnCounter();
west.Increment(West, 3);   // west records +3
west.Decrement(West, 1);   // ...then -1  => west's local view is +2

var east = new PnCounter();
east.Increment(East, 5);   // east records +5, never saw west's writes

Console.WriteLine($"  west (isolated) sees value = {west.Value}");
Console.WriteLine($"  east (isolated) sees value = {east.Value}");

// Merge in BOTH orders on independent copies to prove order-independence.
var westThenEast = PnCounter.Merge(west, east);
var eastThenWest = PnCounter.Merge(east, west);

Console.WriteLine($"  merge(west, east).Value = {westThenEast.Value}");
Console.WriteLine($"  merge(east, west).Value = {eastThenWest.Value}");
Console.WriteLine(
    westThenEast.Value == eastThenWest.Value
        ? $"  [OK] both orders converged deterministically to {westThenEast.Value}"
        : "  [FAIL] divergence!");
Console.WriteLine();

// -- Part 2: OR-Set (add-wins) -------------------------------------------
// An OrSet is an observed-remove set. West adds "green"; East adds then removes
// "green" concurrently. Add-wins semantics mean West's concurrent add survives
// the merge - and again, the outcome does not depend on merge order.
Console.WriteLine("== OR-Set: concurrent add vs add-then-remove of the same element ==");

var green = Encoding.UTF8.GetBytes("green");

var westSet = new OrSet();
westSet.Add(green, West, counter: 1);        // west adds "green"

var eastSet = new OrSet();
eastSet.Add(green, East, counter: 1);        // east adds "green"...
eastSet.Remove(green);                        // ...then removes only the dots it observed

Console.WriteLine($"  west set contains 'green' = {westSet.Contains(green)}");
Console.WriteLine($"  east set contains 'green' = {eastSet.Contains(green)}");

var setWestThenEast = OrSet.Merge(westSet, eastSet);
var setEastThenWest = OrSet.Merge(eastSet, westSet);

Console.WriteLine($"  merge(west, east) contains 'green' = {setWestThenEast.Contains(green)}");
Console.WriteLine($"  merge(east, west) contains 'green' = {setEastThenWest.Contains(green)}");
Console.WriteLine(
    setWestThenEast.Contains(green) == setEastThenWest.Contains(green)
        ? "  [OK] add-wins converged deterministically (west's add survived)"
        : "  [FAIL] divergence!");
Console.WriteLine();

// -- Part 3: land the converged value in a Lattice tree ------------------
// The same LWW algebra backs plain SetAsync/GetAsync on a tree. We store the
// reconciled counter value so you can see the CRDT result persisted in and read
// back from the actual store.
Console.WriteLine("== Persisting the converged counter value into a Lattice tree ==");

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>("conflict-free-demo");

await tree.SetAsync("counter/orders", Encoding.UTF8.GetBytes(westThenEast.Value.ToString()));
var stored = await tree.GetAsync("counter/orders");
Console.WriteLine($"  tree['counter/orders'] = {Encoding.UTF8.GetString(stored!)}");
Console.WriteLine();

Console.WriteLine("Done. Concurrent writers converged without locks, consensus, or a conflict prompt.");

await host.StopAsync();
