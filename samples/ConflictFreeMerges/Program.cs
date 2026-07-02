using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// ConflictFreeMerges
// ---------------------------------------------------------------------------
// Every value in Orleans.Lattice is a CRDT: independent writers can mutate the
// same logical value concurrently, with no coordination, and the store always
// converges to the SAME final state - regardless of thread interleaving or the
// order updates are merged in.
//
// This sample drives that entirely through the typed CRDT *extension* surface
// on ILattice (tree.PnCounter(key), tree.OrSet(key), ...). Those accessors
// read-modify-write a single key through the single-writer leaf seam, so you
// never hand-roll byte arrays, dots, or merge loops - you call the primitive's
// natural verbs (increment, add, enable, tick, set, insert) and read the
// converged result back.
//
// Part 1 proves convergence under real threads (100 writers hammering one tree
// at once). Part 2 tours every CRDT type, showing each converges no matter
// which replica merges first. See docs/lattice/state-primitives.md for the
// merge algebra behind each type.
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
        // OR-Map is an open-shape CRDT: its (key, value) pair is registered
        // per tree so the store knows how to merge each cell. The other CRDT
        // types resolve automatically and need no registration.
        silo.AddOrMapShape<string, PnCounter>("replica-a");
        silo.AddOrMapShape<string, PnCounter>("replica-b");
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grains = host.Services.GetRequiredService<IGrainFactory>();

// A "replica" here is just a distinct Lattice tree. Two trees never share
// state, so writing to each independently models two replicas diverging in
// isolation; merging one into the other models anti-entropy reconciliation.
ILattice Tree(string name) => grains.GetGrain<ILattice>(name);
static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

// ===========================================================================
// Part 1: convergence under concurrent threads
// ===========================================================================
// 100 writers mutate ONE shared tree at the same time. Each writer is its own
// replica id, so a PN-counter increment and an OR-set add from every writer
// must all survive - no lost updates, no locks. The final value is exact.
Console.WriteLine("== 1. Convergence under concurrent threads ==");

const int Writers = 100;
var shared = Tree("crdt-threads");
var votes = shared.PnCounter("votes");
var members = shared.OrSet("members");

var work = new List<Task>(Writers);
for (var i = 0; i < Writers; i++)
{
    var replica = $"writer-{i:D3}";
    work.Add(Task.Run(async () =>
    {
        await votes.IncrementAsync(replica);
        await members.AddAsync(Utf8(replica), replica);
    }));
}

await Task.WhenAll(work);

var finalVotes = await votes.ValueAsync();
var finalMembers = (await members.GetAsync()).Count;
Console.WriteLine($"  {Writers} writers, each its own replica, all writing at once");
Console.WriteLine($"  PnCounter 'votes'   = {finalVotes} (expected {Writers})");
Console.WriteLine($"  OrSet 'members'     = {finalMembers} distinct members (expected {Writers})");
Console.WriteLine(
    finalVotes == Writers && finalMembers == Writers
        ? "  [OK] concurrent writers converged with zero lost updates"
        : "  [FAIL] a concurrent update was lost!");
Console.WriteLine();

// ===========================================================================
// Part 2: every CRDT type converges regardless of merge order
// ===========================================================================
// Two replicas (trees 'replica-a' / 'replica-b') diverge in isolation, then
// exchange full states and each merges the other. Because every merge is
// commutative, associative, and idempotent, both replicas reach an identical
// state whichever side merges first.
Console.WriteLine("== 2. Every CRDT type converges regardless of merge order ==");
Console.WriteLine("  Two replicas diverge in isolation, then merge each other's state.");
Console.WriteLine();

var a = Tree("replica-a");
var b = Tree("replica-b");

// -- PnCounter: an add/subtract counter that sums per-replica components ----
{
    var ca = a.PnCounter("balance");
    var cb = b.PnCounter("balance");
    await ca.IncrementAsync("a", 3);
    await ca.DecrementAsync("a", 1);   // replica-a nets +2
    await cb.IncrementAsync("b", 5);   // replica-b nets +5, never saw a

    var sa = await ca.GetAsync();
    var sb = await cb.GetAsync();
    await ca.MergeAsync(sb);
    await cb.MergeAsync(sa);

    Console.WriteLine("  PnCounter (add/subtract counter)");
    Console.WriteLine($"    after merge: a={await ca.ValueAsync()}  b={await cb.ValueAsync()}  " +
        (await ca.ValueAsync() == await cb.ValueAsync() ? "[OK -> 7]" : "[FAIL]"));
    Console.WriteLine();
}

// -- OrSet: an add-wins set; a concurrent add beats a remove ----------------
{
    var green = Utf8("green");
    var sa = a.OrSet("tags");
    var sb = b.OrSet("tags");
    await sa.AddAsync(green, "a");                 // a adds 'green'
    await sb.AddAsync(green, "b");                 // b adds 'green'...
    await sb.RemoveAsync(green);                   // ...then removes only what it observed

    var setA = await sa.GetAsync();
    var setB = await sb.GetAsync();
    await sa.MergeAsync(setB);
    await sb.MergeAsync(setA);

    Console.WriteLine("  OrSet (add-wins set)");
    Console.WriteLine($"    after merge: a.contains('green')={await sa.ContainsAsync(green)}  " +
        $"b.contains('green')={await sb.ContainsAsync(green)}  [add-wins]");
    Console.WriteLine();
}

// -- OrFlag: enable-wins flag; a concurrent enable beats a disable ----------
// Both replicas start from a shared ENABLED flag (enable on a, merge into b),
// then a disables while b re-enables concurrently.
{
    var fa = a.OrFlag("feature");
    var fb = b.OrFlag("feature");
    await fa.EnableAsync("a");
    await fb.MergeAsync(await fa.GetAsync());       // b now observes the same enable
    await fa.DisableAsync();                        // a turns it off
    await fb.EnableAsync("b");                       // b turns it on again, concurrently

    var flagA = await fa.GetAsync();
    var flagB = await fb.GetAsync();
    await fa.MergeAsync(flagB);
    await fb.MergeAsync(flagA);

    Console.WriteLine("  OrFlag (enable-wins flag)");
    Console.WriteLine($"    after merge: a.enabled={await fa.IsEnabledAsync()}  " +
        $"b.enabled={await fb.IsEnabledAsync()}  [enable-wins -> True]");
    Console.WriteLine();
}

// -- RwFlag: disable-wins flag; same race, opposite winner ------------------
{
    var fa = a.RwFlag("access");
    var fb = b.RwFlag("access");
    await fa.EnableAsync("a");
    await fb.MergeAsync(await fa.GetAsync());        // b observes the same enable
    await fa.DisableAsync("a");                      // a revokes
    await fb.EnableAsync("b");                        // b re-grants, concurrently

    var flagA = await fa.GetAsync();
    var flagB = await fb.GetAsync();
    await fa.MergeAsync(flagB);
    await fb.MergeAsync(flagA);

    Console.WriteLine("  RwFlag (disable-wins flag)");
    Console.WriteLine($"    after merge: a.enabled={await fa.IsEnabledAsync()}  " +
        $"b.enabled={await fb.IsEnabledAsync()}  [disable-wins -> False]");
    Console.WriteLine();
}

// -- VersionVector: a causal clock; merge is pointwise-max per replica -------
{
    var va = a.VersionVector("causal");
    var vb = b.VersionVector("causal");
    await va.TickAsync("a");
    await va.TickAsync("a");     // replica-a advances its own lane twice
    await vb.TickAsync("b");     // replica-b advances its lane once (concurrent)

    var vecA = await va.GetAsync();
    var vecB = await vb.GetAsync();
    await va.MergeAsync(vecB);
    await vb.MergeAsync(vecA);

    var mergedA = await va.GetAsync();
    var mergedB = await vb.GetAsync();
    Console.WriteLine("  VersionVector (causal version tracker)");
    Console.WriteLine($"    after merge: a.replicas={mergedA.Entries.Count}  b.replicas={mergedB.Entries.Count}  " +
        $"identical={mergedA.DominatesOrEquals(mergedB) && mergedB.DominatesOrEquals(mergedA)}  [both lanes kept]");
    Console.WriteLine();
}

// -- MvRegister: keeps concurrent writes as a conflict set (not LWW) --------
{
    var ra = a.MvRegister<string>("profile");
    var rb = b.MvRegister<string>("profile");
    await ra.SetAsync("a", "left-edit");    // two replicas edit the same
    await rb.SetAsync("b", "right-edit");   // register concurrently

    var regA = await ra.GetAsync();
    var regB = await rb.GetAsync();
    await ra.MergeAsync(regB);
    await rb.MergeAsync(regA);

    var valuesA = string.Join(", ", await ra.ValuesAsync());
    var valuesB = string.Join(", ", await rb.ValuesAsync());
    Console.WriteLine("  MvRegister (multi-value register)");
    Console.WriteLine($"    after merge: a=[{valuesA}]  b=[{valuesB}]  [both edits survive for the app to resolve]");
    Console.WriteLine();
}

// -- OrMap: an add-wins map whose cells are themselves CRDTs -----------------
// The map is driven via the extension; each cell value is a small PnCounter
// payload (map cells must be CRDTs, so we compose one for the value).
{
    var ma = a.OrMap<string, PnCounter>("tallies");
    var mb = b.OrMap<string, PnCounter>("tallies");

    var londonCount = new PnCounter();
    londonCount.Increment("a", 10);
    var parisCount = new PnCounter();
    parisCount.Increment("b", 4);

    await ma.SetAsync("london", "a", londonCount);   // a adds the 'london' tally
    await mb.SetAsync("paris", "b", parisCount);     // b adds the 'paris' tally

    var mapA = await ma.GetAsync();
    var mapB = await mb.GetAsync();
    await ma.MergeAsync(mapB);
    await mb.MergeAsync(mapA);

    Console.WriteLine("  OrMap (map of CRDT cells)");
    Console.WriteLine($"    after merge: a has london={await ma.ContainsKeyAsync("london")} paris={await ma.ContainsKeyAsync("paris")}  " +
        $"b has london={await mb.ContainsKeyAsync("london")} paris={await mb.ContainsKeyAsync("paris")}");
    Console.WriteLine();
}

// -- Sequence (RGA): an ordered list; concurrent inserts converge on order --
{
    var sa = a.Sequence<string>("timeline");
    var sb = b.Sequence<string>("timeline");
    await sa.InsertAtAsync(0, "a", "a1");
    await sa.InsertAtAsync(1, "a", "a2");   // replica-a builds [a1, a2]
    await sb.InsertAtAsync(0, "b", "b1");   // replica-b inserts [b1] concurrently

    var seqA = await sa.GetAsync();
    var seqB = await sb.GetAsync();
    await sa.MergeAsync(seqB);
    await sb.MergeAsync(seqA);

    var listA = string.Join(", ", await sa.ToListAsync());
    var listB = string.Join(", ", await sb.ToListAsync());
    Console.WriteLine("  Sequence / RGA (ordered list)");
    Console.WriteLine($"    after merge: a=[{listA}]  b=[{listB}]  identical={listA == listB}  [deterministic order]");
    Console.WriteLine();
}

Console.WriteLine("Done. Every replica reached the same state without locks, consensus, or a conflict prompt.");

await host.StopAsync();
