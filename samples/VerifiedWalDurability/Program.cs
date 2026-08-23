using Orleans.Lattice;
using Orleans.Lattice.Primitives;

// ---------------------------------------------------------------------------
// VerifiedWalDurability - the observable cursor-safety properties that the
// WAL cursor-registry cores, the Coyote concurrency tier, and the guard tests
// machine-check.
//
// The write-ahead log's garbage collector may only trim log entries that every
// consumer has durably acked, and it learns each consumer's progress from the
// WAL cursor registry. Two registry properties keep the GC safe under the
// concurrent, out-of-order, sometimes-redelivered cursor reports a live cluster
// produces:
//
//   * per-consumer monotonicity - a consumer's acked cursor is a max-merge, so a
//     stale or duplicate re-delivery of an OLDER cursor never regresses it, and
//   * the min-cursor floor - the GC-visible trim floor is the MINIMUM cursor
//     across all consumers, so a fast consumer racing ahead never lets the GC
//     trim past a slower one and strand it.
//
// This sample makes both properties observable at runtime by driving the REAL
// production registry (InMemoryWalCursorRegistry) under concurrency and checking
// that neither property is ever violated. Those are exactly the properties that:
//   * the production registry max-merge and min-cursor scan decide on the hot
//     path (src/lattice/InMemoryWalCursorRegistry.cs),
//   * the Coyote models assert under every interleaving
//     (test/lattice/BPlusTree/Coyote/WalCursorMonotonicityModel.cs and
//      WalGcTrimFloorModel.cs), and
//   * the WalGcTrimCore trim-eligibility predicate enforces for the GC
//     (src/lattice/WalGcTrimCore.cs).
// See docs/lattice/verified-wal.md.
// ---------------------------------------------------------------------------

const string tree = "orders";

// The real production registry - the same type AddWalCursorRegistry wires into a
// silo. No Orleans host is needed: it is a plain, thread-safe public class, and
// driving it directly is what makes the verified property observable here.
var registry = new InMemoryWalCursorRegistry();

static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

Console.WriteLine("== VerifiedWalDurability sample ==");
Console.WriteLine();

// --- 1. Per-consumer cursor monotonicity under concurrent stale re-delivery ---
// Several consumers each advance their cursor while a chaotic reporter task
// re-delivers OLDER cursors for random consumers (a duplicate/late report, which
// a real transport can produce). A monitor samples the registry and checks that
// no consumer's cursor is ever observed to move backwards. The max-merge in
// ReportCursorAsync makes a stale report a no-op.
Console.WriteLine("1) Racing concurrent cursor reports (with stale re-deliveries) against the registry...");

var consumers = new[] { "ship:eu", "ship:us", "materialiser", "backup" };
const int advances = 400;
using var done = new CancellationTokenSource();

// Highest cursor we have *legitimately* advanced each consumer to; the monitor
// asserts the registry never reports a consumer below the value it already hit.
var highWater = new Dictionary<string, long>(StringComparer.Ordinal);
foreach (var c in consumers)
{
    highWater[c] = 0;
}

long samples = 0, regressions = 0, staleReportsSent = 0;

var monitor = Task.Run(async () =>
{
    long lastSeen = 0;
    var seenPerConsumer = new Dictionary<string, long>(StringComparer.Ordinal);
    while (!done.IsCancellationRequested)
    {
        var snapshot = await registry.SnapshotAsync(tree);
        foreach (var entry in snapshot)
        {
            samples++;
            var prev = seenPerConsumer.TryGetValue(entry.ConsumerId, out var v) ? v : 0;
            if (entry.Cursor.WallClockTicks < prev)
            {
                regressions++; // a consumer's acked cursor moved backwards: a torn watermark
            }
            seenPerConsumer[entry.ConsumerId] = Math.Max(prev, entry.Cursor.WallClockTicks);
        }

        lastSeen++;
    }
});

var rng = new Random(17);
for (var i = 1; i <= advances; i++)
{
    // Advance every consumer forward by a jittered step.
    foreach (var c in consumers)
    {
        var next = highWater[c] + 1 + rng.Next(0, 3);
        highWater[c] = next;
        await registry.ReportCursorAsync(tree, c, Hlc(next));
    }

    // Occasionally re-deliver an OLDER cursor for a random consumer. A correct
    // registry max-merges it away, so it must not regress the consumer.
    if (rng.Next(0, 2) == 0)
    {
        var victim = consumers[rng.Next(consumers.Length)];
        var stale = Math.Max(1, highWater[victim] - rng.Next(1, 20));
        staleReportsSent++;
        await registry.ReportCursorAsync(tree, victim, Hlc(stale));
    }
}

done.Cancel();
await monitor;

Console.WriteLine($"   Cursor advances issued : {advances * consumers.Length}");
Console.WriteLine($"   Stale re-deliveries    : {staleReportsSent}");
Console.WriteLine($"   Registry samples taken : {samples}");
Console.WriteLine($"   Per-consumer REGRESSIONS: {regressions}");
Console.WriteLine(regressions == 0
    ? "   -> zero regressions: every stale re-delivery was max-merged away; each cursor only advanced."
    : "   -> UNEXPECTED cursor regression detected!");
Console.WriteLine();

// --- 2. The GC min-cursor floor never strands a lagging consumer --------------
// The GC may only trim past the SLOWEST consumer. GetMinCursorAsync returns the
// minimum cursor across all consumers, so racing the fast consumers far ahead
// must NOT raise the trim floor above the laggard until the laggard itself
// advances. We pin one slow consumer, sprint the others, and confirm the floor
// stays pinned to the laggard.
Console.WriteLine("2) Pinning a slow consumer while the others sprint ahead...");

var registry2 = new InMemoryWalCursorRegistry();
await registry2.ReportCursorAsync(tree, "laggard", Hlc(100));
await registry2.ReportCursorAsync(tree, "fast:a", Hlc(100));
await registry2.ReportCursorAsync(tree, "fast:b", Hlc(100));

// Sprint the two fast consumers far ahead; leave the laggard pinned at 100.
for (var t = 101; t <= 5000; t++)
{
    await registry2.ReportCursorAsync(tree, "fast:a", Hlc(t));
    await registry2.ReportCursorAsync(tree, "fast:b", Hlc(t));
}

var floorWhilePinned = await registry2.GetMinCursorAsync(tree);
Console.WriteLine($"   fast:a, fast:b acked   : 5000");
Console.WriteLine($"   laggard acked          : 100");
Console.WriteLine($"   GC trim floor (min)    : {floorWhilePinned?.WallClockTicks.ToString() ?? "null"}");
Console.WriteLine(floorWhilePinned?.WallClockTicks == 100
    ? "   -> floor pinned to the laggard: the GC cannot trim past the slowest consumer."
    : "   -> UNEXPECTED floor above the laggard: a slow consumer could be stranded!");
Console.WriteLine();

// Now advance the laggard; the floor is free to rise to the new minimum.
await registry2.ReportCursorAsync(tree, "laggard", Hlc(4200));
var floorAfterCatchUp = await registry2.GetMinCursorAsync(tree);
Console.WriteLine($"   laggard catches up to  : 4200");
Console.WriteLine($"   GC trim floor (min)    : {floorAfterCatchUp?.WallClockTicks.ToString() ?? "null"}");
Console.WriteLine(floorAfterCatchUp?.WallClockTicks == 4200
    ? "   -> floor advanced to the new minimum only after the laggard reported: safe forward progress."
    : "   -> UNEXPECTED floor value after the laggard advanced!");
Console.WriteLine();

Console.WriteLine("These WAL cursor-safety properties are machine-checked, not just observed here:");
Console.WriteLine("  * cores    : src/lattice/InMemoryWalCursorRegistry.cs, src/lattice/WalGcTrimCore.cs");
Console.WriteLine("  * Coyote   : test/lattice/BPlusTree/Coyote/  (dotnet test --filter Category=Coyote)");
Console.WriteLine("  * docs     : docs/lattice/verified-wal.md");
Console.WriteLine();
Console.WriteLine("Done.");
