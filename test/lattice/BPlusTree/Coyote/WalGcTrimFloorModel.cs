using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which trim-floor rule a <see cref="WalGcTrimFloorModel"/> run drives, so the
/// safety test can prove the min-cursor floor is load-bearing by removing it
/// (flooring under the <em>fastest</em> consumer instead of the slowest) and
/// asserting Coyote re-finds a lagging consumer trimmed off its own WAL tail.
/// </summary>
public enum WalGcTrimFloorMode
{
    /// <summary>
    /// The fix: the GC floors its trim point under the <b>minimum</b> cursor
    /// across every reporting consumer, exactly as
    /// <c>IWalCursorRegistry.GetMinCursorAsync</c> feeds
    /// <see cref="Orleans.Lattice.WalGcTrimCore"/>. A partitioned or lagging
    /// consumer therefore holds the trim frontier stationary and never falls off
    /// the log, no matter how the interleaving advances the faster consumers.
    /// </summary>
    MinCursorFloor,

    /// <summary>
    /// The guard removed: the GC floors under the <b>maximum</b> cursor (as if a
    /// lagging peer were dropped from the min), so once consumers diverge the GC
    /// trims past a slow consumer's frontier and strands it below the oldest
    /// retained entry - the fall-off-the-log data-loss class the
    /// <c>WalTrimUnderShippingChaosTests</c> chaos suite probes stochastically.
    /// </summary>
    MaxCursorFloorNoLaggard,
}

/// <summary>
/// A Coyote concurrency model of the WAL garbage collector's trim-floor seam
/// under multiple consumers advancing their cursors concurrently with GC passes,
/// driving the <b>production</b> eligibility rule
/// (<see cref="Orleans.Lattice.WalGcTrimCore.IsEntryEligible"/>) that
/// <c>LatticeWalGc.TrimShardAsync</c> applies to every entry it scans. Because
/// the model executes the same per-entry decision Orleans runs, a violation
/// Coyote finds is a violation of the real trim path.
/// <para>
/// The scenario models a dense, append-only WAL of <see cref="EntryCount"/>
/// entries (offset <c>i</c>, HLC tick <c>i + 1</c>) and <c>consumerCount</c>
/// consumers, each of which must eventually acknowledge every entry. The Coyote
/// scheduler interleaves {advance a scheduler-chosen consumer by one entry, run
/// a GC pass} so a GC pass can observe the consumers at any relative progress -
/// including one consumer stalled arbitrarily far behind the others, the
/// partitioned-peer case.
/// </para>
/// <para>
/// The safety property is <b>never trim past the slowest consumer</b>: after any
/// GC pass trims the head through offset <c>T</c>, every consumer must already
/// have acknowledged all of <c>[0, T)</c>. Trimming an entry a consumer still
/// needs strands it below the oldest retained offset - a silent, permanent
/// fall-off-the-log gap that forces a full re-bootstrap.
/// </para>
/// </summary>
public sealed class WalGcTrimFloorModel : ICoyoteModel
{
    /// <summary>The dense WAL length every consumer must fully acknowledge.</summary>
    private const int EntryCount = 3;

    private readonly int _consumerCount;
    private readonly WalGcTrimFloorMode _mode;

    /// <summary>
    /// Creates the model for <paramref name="consumerCount"/> concurrently
    /// advancing consumers under the chosen trim-floor <paramref name="mode"/>.
    /// </summary>
    public WalGcTrimFloorModel(int consumerCount, WalGcTrimFloorMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(consumerCount, 2);
        _consumerCount = consumerCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // Every consumer's cursor counts the entries it has acknowledged: cursor
        // c means it has consumed offsets [0, c) (HLC ticks 1..c) and still needs
        // offset >= c. All start at 0 (HLC.Zero, "no cursor reported").
        var cursors = new long[_consumerCount];
        var trimmed = 0L;

        // Each consumer must advance EntryCount times. One guaranteed advance per
        // iteration bounds the loop, while an optional GC pass before and after
        // each advance lets the scheduler observe the consumers at every relative
        // progress point (including a single consumer stalled far behind).
        var remainingAdvances = _consumerCount * EntryCount;
        while (remainingAdvances > 0)
        {
            if (runtime.RandomBoolean())
            {
                trimmed = GcPass(cursors, trimmed);
            }

            var c = SelectAdvanceable(cursors, runtime);
            cursors[c]++;
            remainingAdvances--;

            if (runtime.RandomBoolean())
            {
                trimmed = GcPass(cursors, trimmed);
            }
        }

        // Once every consumer has acknowledged the whole log, a final pass must
        // reclaim it all: nothing was lost, only deferred behind the slowest
        // consumer (a bounded-progress liveness check).
        trimmed = GcPass(cursors, trimmed);
        Specification.Assert(
            trimmed == EntryCount,
            $"GC did not reclaim the fully-acknowledged log: trimmed={trimmed} of {EntryCount} "
            + "(every entry must be trimmable once the slowest consumer catches up)");
    }

    /// <summary>
    /// One GC pass. Samples the trim floor (the minimum consumer cursor under the
    /// production rule, or the maximum under the removed-guard mode), scans the
    /// dense WAL head ascending through the production eligibility core - stopping
    /// at the first entry it rejects - trims the head through the eligible prefix,
    /// and asserts the trim never passed the slowest consumer.
    /// </summary>
    private long GcPass(long[] cursors, long trimmed)
    {
        var floorTick = _mode == WalGcTrimFloorMode.MinCursorFloor
            ? Min(cursors)
            // The removed guard: floor under the fastest consumer, as if the
            // laggard were dropped from the min-cursor computation.
            : Max(cursors);

        // A zero floor is HLC.Zero: the production core disables the cursor
        // clause below it (nothing is trimmed by cursor), so pass null exactly as
        // the registry yields no min cursor.
        HybridLogicalClock? minCursor = floorTick > 0
            ? new HybridLogicalClock { WallClockTicks = floorTick }
            : null;

        var next = trimmed;
        for (var offset = trimmed; offset < EntryCount; offset++)
        {
            var entryTimestamp = new HybridLogicalClock { WallClockTicks = offset + 1 };

            // Drive the real production predicate: retention (TTL), causal-stable,
            // and blocked-floor clauses are inactive here so the model isolates
            // the cursor floor the chaos suite pins.
            if (!Orleans.Lattice.WalGcTrimCore.IsEntryEligible(
                    entryTimestamp,
                    entryVectorClock: null,
                    minCursor,
                    ttlCeiling: null,
                    causalStable: null,
                    blockedFloor: null))
            {
                break;
            }

            next = offset + 1;
        }

        // No-fall-off safety: trimming the head through `next` reclaims every
        // offset below it, so every consumer must already have acknowledged all
        // of [0, next). A consumer whose cursor is below `next` still needs a
        // trimmed entry and has been stranded off its own WAL tail.
        var slowest = Min(cursors);
        Specification.Assert(
            next <= slowest,
            $"GC trimmed the WAL head through {next} past the slowest consumer cursor {slowest}: "
            + "a lagging or partitioned consumer was stranded below the oldest retained entry "
            + "(fall-off-the-log replication gap)");

        return next;
    }

    /// <summary>
    /// Picks which consumer advances next, driving the choice through the runtime
    /// so the harness explores every relative-progress interleaving. Scans for a
    /// not-yet-complete consumer and takes the first the runtime accepts,
    /// defaulting to the first incomplete one.
    /// </summary>
    private int SelectAdvanceable(long[] cursors, ICoyoteRuntime runtime)
    {
        var fallback = -1;
        for (var i = 0; i < cursors.Length; i++)
        {
            if (cursors[i] >= EntryCount)
            {
                continue;
            }

            if (fallback < 0)
            {
                fallback = i;
            }

            if (runtime.RandomBoolean())
            {
                return i;
            }
        }

        return fallback;
    }

    private static long Min(long[] values)
    {
        var min = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            if (values[i] < min)
            {
                min = values[i];
            }
        }

        return min;
    }

    private static long Max(long[] values)
    {
        var max = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            if (values[i] > max)
            {
                max = values[i];
            }
        }

        return max;
    }
}
