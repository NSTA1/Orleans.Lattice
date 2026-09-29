using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three per-element trims on the observed-remove CRDT dot primitives,
/// so their time and byte deltas are measurable in the clear rather than buried
/// under a silo, a grain call and a transport.
/// <para>
/// (1) <b>Span scans in the dot primitives.</b> Every hot loop in
/// <c>OrSetDotCompaction</c> walked its <c>List&lt;OrSetDot&gt;</c> through the
/// list indexer. <c>OrSetDot</c> is a struct, so each <c>list[i]</c> copied the
/// whole dot out and re-checked bounds, and <c>List&lt;T&gt;.Count</c> is a
/// mutable field the JIT cannot hoist out of a loop condition. The loops now
/// take a span once and read through <c>ref readonly</c>. No loop changes the
/// list's length while iterating, so the rewrite is purely mechanical.
/// </para>
/// <para>
/// (2) <b>Liveness reads answer "any", not "how many".</b> Every consumer of
/// the OR-set / RW-set / RW-flag live-dot helpers compared the count to zero,
/// so counting was strictly more work than the answer needed: the walk can stop
/// at the first survivor instead of testing every remaining dot against the
/// whole cancelling list.
/// </para>
/// <para>
/// (3) <b>Merge-time compaction sweep.</b> <c>Compact()</c> runs on every
/// mutation and merge. It paired each add list with its tombstone list, which
/// bought a hash probe of the tombstone map per element - over a base64 element
/// key, so a full string hash - and then compacted a tombstone list the
/// following sweep compacted again. Compaction of one list depends on nothing
/// but that list, so the maps are now swept independently.
/// </para>
/// <para>
/// Read all three groups for <b>time</b>: none of them changes heap traffic on
/// the common shape, so a claimed byte win would be noise, and the
/// <c>Allocated</c> column is reported precisely so that can be checked rather
/// than asserted. Every group carries a control lane where the trim is expected
/// to buy little or nothing - a short cover list, an un-churned CRDT shape, and
/// an already-normal-form compaction corpus - because a trim that taxes the
/// common case to help the rare one is not a trim.
/// </para>
/// <para>
/// Every baseline lane is a verbatim copy of the code the trim replaced, so it
/// pays exactly the overhead its shipped counterpart pays, and each is asserted
/// in <see cref="Setup"/> to produce exactly the answer its counterpart
/// produces - over corpora that include multi-replica dot lists, a counter
/// collision across replicas, an all-superseded list that compaction must
/// actually shrink, and an empty list. A lane that answers differently is
/// measuring different work, and the comparison would be void.
/// </para>
/// <para>
/// The compaction corpora are deliberately already at the bounded normal form
/// (one dot per replica per element). <c>Compact</c> is idempotent and is
/// re-run on every mutation and merge, so normal form is both the steady-state
/// shape and the one that keeps every iteration's cost identical; a corpus that
/// shrank on the first iteration would measure a different thing thereafter.
/// The <c>Superseded</c> lanes cover the shrinking case separately, over a
/// fresh clone per invocation.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtdotscantrims</c> (or
/// <c>--suite crdtdotscantrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDotScanTrimsBenchmarks
{
    private const int ElementCount = 64;
    private const int SlotCount = 256;
    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    /// <summary>
    /// Mirrors the private <c>OrSetDotCompaction.ReplicaScanThreshold</c> so the
    /// baseline takes the dictionary tail on exactly the same inputs.
    /// </summary>
    private const int BaselineReplicaScanThreshold = 8;

    /// <summary>
    /// Mirrors the private <c>OrSetDotCompaction.CoverCollapseThreshold</c>. The
    /// span-group corpora keep cover lists at or below it so both lanes take the
    /// linear scan the group is measuring, not the collapsed counter test.
    /// </summary>
    private const int BaselineCoverCollapseThreshold = 8;

    // Group 1 - span scans.
    private List<OrSetDot>[] _scanDots = null!;
    private List<OrSetDot>[] _scanCovers = null!;
    private List<OrSetDot>[] _normalFormSlots = null!;
    private List<OrSetDot>[] _supersededSlots = null!;

    // Group 2 - liveness.
    private OrSet _churnedSet = null!;
    private OrSet _quietSet = null!;
    private RwFlag _churnedFlag = null!;
    private RwFlag _quietFlag = null!;

    // Group 3 - compaction sweeps.
    private OrSet _normalFormOrSet = null!;
    private RwSet _normalFormRwSet = null!;

    [GlobalSetup]
    public void Setup()
    {
        _scanDots = new List<OrSetDot>[SlotCount];
        _scanCovers = new List<OrSetDot>[SlotCount];
        _normalFormSlots = new List<OrSetDot>[SlotCount];
        _supersededSlots = new List<OrSetDot>[SlotCount];
        for (var i = 0; i < SlotCount; i++)
        {
            _scanDots[i] = MakeMultiReplicaSlot(6, baseCounter: 10);
            _scanCovers[i] = MakeMultiReplicaSlot(BaselineCoverCollapseThreshold, baseCounter: 1);
            _normalFormSlots[i] = MakeMultiReplicaSlot(6, baseCounter: 10);
            _supersededSlots[i] = MakeSupersededSlot(12);
        }

        _churnedSet = MakeOrSet(24, 20, ReplicaA, ReplicaA);
        _quietSet = MakeOrSet(3, 2, ReplicaA, ReplicaA);
        _churnedFlag = MakeRwFlag(24, 20);
        _quietFlag = MakeRwFlag(3, 2);
        _normalFormOrSet = MakeNormalFormOrSet();
        _normalFormRwSet = MakeNormalFormRwSet();

        // --- Group 1 equivalence, including the shapes that could diverge. ---
        AssertScanEquivalent(_scanDots, _scanCovers, "multi-replica scan");
        AssertScanEquivalent(_normalFormSlots, _scanCovers, "normal-form scan");
        AssertScanEquivalent([[]], [MakeMultiReplicaSlot(3, 1)], "empty dots");
        AssertScanEquivalent([MakeMultiReplicaSlot(3, 1)], [[]], "empty cover");
        AssertScanEquivalent(
            [[new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }]],
            [[new OrSetDot { ReplicaId = ReplicaA, Counter = 5 }]],
            "counter collision across replicas");

        AssertCompactScanEquivalent(MakeMultiReplicaSlot(6, 10), "normal form");
        AssertCompactScanEquivalent(MakeSupersededSlot(12), "all superseded");
        AssertCompactScanEquivalent(MakeManyReplicaSlot(BaselineReplicaScanThreshold * 3), "over scan threshold");
        AssertCompactScanEquivalent([], "empty");
        AssertCompactScanEquivalent([new OrSetDot { ReplicaId = ReplicaA, Counter = 1 }], "single dot");

        // --- Group 2 equivalence. ---
        AssertLivenessEquivalent(_churnedSet, "churned");
        AssertLivenessEquivalent(_quietSet, "quiet");
        AssertLivenessEquivalent(MakeCounterCollisionOrSet(), "counter collision");
        AssertFlagEquivalent(_churnedFlag, "churned flag");
        AssertFlagEquivalent(_quietFlag, "quiet flag");
        AssertFlagEquivalent(MakeCounterCollisionRwFlag(), "counter collision flag");

        // --- Group 3 equivalence. ---
        AssertOrSetCompactionEquivalent(MakeNormalFormOrSet(), "normal form");
        AssertOrSetCompactionEquivalent(MakeOrSet(24, 20, ReplicaA, ReplicaA), "churned");
        AssertRwSetCompactionEquivalent(MakeNormalFormRwSet(), "normal form");
    }

    // ---------------------------------------------------------------------
    // Group 1 - span scans vs the list indexer.
    // ---------------------------------------------------------------------

    /// <summary>The cancellation predicate, the innermost loop of every liveness read.</summary>
    [Benchmark]
    public int CoversScan_Baseline_ListIndexer()
    {
        var live = 0;
        for (var i = 0; i < _scanDots.Length; i++)
        {
            var dots = _scanDots[i];
            var cover = _scanCovers[i];
            for (var d = 0; d < dots.Count; d++)
            {
                var dot = dots[d];
                if (!CoversByIndexer(cover, in dot)) live++;
            }
        }

        return live;
    }

    [Benchmark]
    public int CoversScan_Optimized_Span()
    {
        var live = 0;
        for (var i = 0; i < _scanDots.Length; i++)
        {
            var dots = _scanDots[i];
            var cover = _scanCovers[i];
            for (var d = 0; d < dots.Count; d++)
            {
                var dot = dots[d];
                if (!OrSetDotCompaction.Covers(cover, in dot)) live++;
            }
        }

        return live;
    }

    /// <summary>Compaction over an already-normal-form slot: the steady-state shape.</summary>
    [Benchmark]
    public int CompactScan_Baseline_ListIndexer()
    {
        var changed = 0;
        for (var i = 0; i < _normalFormSlots.Length; i++)
        {
            if (CompactMaxPerReplicaByIndexer(_normalFormSlots[i])) changed++;
        }

        return changed;
    }

    [Benchmark]
    public int CompactScan_Optimized_Span()
    {
        var changed = 0;
        for (var i = 0; i < _normalFormSlots.Length; i++)
        {
            if (OrSetDotCompaction.CompactMaxPerReplica(_normalFormSlots[i])) changed++;
        }

        return changed;
    }

    /// <summary>
    /// The shrinking case, over a fresh clone per invocation so both lanes see
    /// the same un-compacted input every time.
    /// </summary>
    [Benchmark]
    public int CompactSuperseded_Baseline_ListIndexer()
    {
        var changed = 0;
        for (var i = 0; i < _supersededSlots.Length; i++)
        {
            if (CompactMaxPerReplicaByIndexer([.. _supersededSlots[i]])) changed++;
        }

        return changed;
    }

    [Benchmark]
    public int CompactSuperseded_Optimized_Span()
    {
        var changed = 0;
        for (var i = 0; i < _supersededSlots.Length; i++)
        {
            if (OrSetDotCompaction.CompactMaxPerReplica([.. _supersededSlots[i]])) changed++;
        }

        return changed;
    }

    // ---------------------------------------------------------------------
    // Group 2 - liveness reads answer "any", not "how many".
    // ---------------------------------------------------------------------

    [Benchmark]
    public int OrSetCountChurned_Baseline_Counting() => CountOrSetByCounting(_churnedSet);

    [Benchmark]
    public int OrSetCountChurned_Optimized_Any() => _churnedSet.Count;

    [Benchmark]
    public int OrSetCountQuiet_Baseline_Counting() => CountOrSetByCounting(_quietSet);

    [Benchmark]
    public int OrSetCountQuiet_Optimized_Any() => _quietSet.Count;

    [Benchmark]
    public bool RwFlagEnabledChurned_Baseline_Counting()
        => _churnedFlag.Enables.Count > 0
            && OrSetDotCompaction.CountLive(_churnedFlag.Disables, _churnedFlag.Tombstones) == 0;

    [Benchmark]
    public bool RwFlagEnabledChurned_Optimized_Any() => _churnedFlag.IsEnabled;

    [Benchmark]
    public bool RwFlagEnabledQuiet_Baseline_Counting()
        => _quietFlag.Enables.Count > 0
            && OrSetDotCompaction.CountLive(_quietFlag.Disables, _quietFlag.Tombstones) == 0;

    [Benchmark]
    public bool RwFlagEnabledQuiet_Optimized_Any() => _quietFlag.IsEnabled;

    // ---------------------------------------------------------------------
    // Group 3 - independent compaction sweeps.
    // ---------------------------------------------------------------------

    [Benchmark]
    public int OrSetCompact_Baseline_PairedProbe()
    {
        CompactOrSetPaired(_normalFormOrSet);
        return _normalFormOrSet.Adds.Count;
    }

    [Benchmark]
    public int OrSetCompact_Optimized_IndependentSweeps()
    {
        _normalFormOrSet.Compact();
        return _normalFormOrSet.Adds.Count;
    }

    [Benchmark]
    public int RwSetCompact_Baseline_PairedProbe()
    {
        CompactRwSetPaired(_normalFormRwSet);
        return _normalFormRwSet.Adds.Count;
    }

    [Benchmark]
    public int RwSetCompact_Optimized_IndependentSweeps()
    {
        _normalFormRwSet.Compact();
        return _normalFormRwSet.Adds.Count;
    }

    // ---------------------------------------------------------------------
    // Baselines - verbatim copies of the code each trim replaced.
    // ---------------------------------------------------------------------

    /// <summary>The prior <c>OrSetDotCompaction.Covers</c> body.</summary>
    private static bool CoversByIndexer(List<OrSetDot> cover, in OrSetDot dot)
    {
        for (var i = 0; i < cover.Count; i++)
        {
            var candidate = cover[i];
            if (candidate.Counter >= dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The prior <c>OrSetDotCompaction.CompactMaxPerReplica</c> body.</summary>
    private static bool CompactMaxPerReplicaByIndexer(List<OrSetDot> dots)
    {
        if (dots.Count <= 1 || OrSetDotCompaction.CompactionDisabled)
        {
            return false;
        }

        var write = 0;
        for (var read = 0; read < dots.Count; read++)
        {
            var dot = dots[read];
            var superseded = false;
            for (var kept = 0; kept < write; kept++)
            {
                if (!string.Equals(dots[kept].ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                if (dots[kept].Counter < dot.Counter)
                {
                    dots[kept] = dot;
                }

                superseded = true;
                break;
            }

            if (!superseded)
            {
                dots[write++] = dot;
                if (write > BaselineReplicaScanThreshold)
                {
                    return CompactManyReplicasByIndexer(dots, write, read + 1);
                }
            }
        }

        if (write == dots.Count)
        {
            return false;
        }

        dots.RemoveRange(write, dots.Count - write);
        return true;
    }

    /// <summary>The prior <c>OrSetDotCompaction.CompactManyReplicas</c> body.</summary>
    private static bool CompactManyReplicasByIndexer(List<OrSetDot> dots, int write, int read)
    {
        var slotByReplica = new Dictionary<string, int>(write, StringComparer.Ordinal);
        for (var i = 0; i < write; i++)
        {
            slotByReplica[dots[i].ReplicaId] = i;
        }

        for (; read < dots.Count; read++)
        {
            var dot = dots[read];
            if (slotByReplica.TryGetValue(dot.ReplicaId, out var slot))
            {
                if (dots[slot].Counter < dot.Counter)
                {
                    dots[slot] = dot;
                }

                continue;
            }

            slotByReplica[dot.ReplicaId] = write;
            dots[write++] = dot;
        }

        if (write == dots.Count)
        {
            return false;
        }

        dots.RemoveRange(write, dots.Count - write);
        return true;
    }

    /// <summary>The prior <c>OrSet.Count</c> body: count live dots, compare to zero.</summary>
    private static int CountOrSetByCounting(OrSet set)
    {
        var n = 0;
        if (set.Tombstones.Count == 0)
        {
            foreach (var dots in set.Adds.Values)
            {
                if (dots.Count > 0) n++;
            }

            return n;
        }

        foreach (var (key, dots) in set.Adds)
        {
            set.Tombstones.TryGetValue(key, out var tomb);
            var live = tomb is null ? dots.Count : OrSetDotCompaction.CountLive(dots, tomb);
            if (live > 0) n++;
        }

        return n;
    }

    /// <summary>The prior <c>OrSet.Compact</c> body.</summary>
    private static void CompactOrSetPaired(OrSet set)
    {
        foreach (var (key, dots) in set.Adds)
        {
            set.Tombstones.TryGetValue(key, out var tomb);
            OrSetDotCompaction.CompactMaxPerReplica(dots);
            if (tomb is not null) OrSetDotCompaction.CompactMaxPerReplica(tomb);
        }

        foreach (var dots in set.Tombstones.Values)
        {
            OrSetDotCompaction.CompactMaxPerReplica(dots);
        }
    }

    /// <summary>The prior <c>RwSet.Compact</c> body.</summary>
    private static void CompactRwSetPaired(RwSet set)
    {
        foreach (var (key, dots) in set.Adds)
        {
            set.Removes.TryGetValue(key, out var removes);
            set.Tombstones.TryGetValue(key, out var tomb);
            OrSetDotCompaction.CompactMaxPerReplica(dots);
            if (removes is not null) OrSetDotCompaction.CompactMaxPerReplica(removes);
            if (tomb is not null) OrSetDotCompaction.CompactMaxPerReplica(tomb);
        }

        foreach (var (key, removes) in set.Removes)
        {
            set.Tombstones.TryGetValue(key, out var tomb);
            OrSetDotCompaction.CompactMaxPerReplica(removes);
            if (tomb is not null) OrSetDotCompaction.CompactMaxPerReplica(tomb);
        }

        foreach (var dots in set.Tombstones.Values)
        {
            OrSetDotCompaction.CompactMaxPerReplica(dots);
        }
    }

    // ---------------------------------------------------------------------
    // Corpora.
    // ---------------------------------------------------------------------

    /// <summary>One dot per replica, already at normal form, distinct replica ids.</summary>
    private static List<OrSetDot> MakeMultiReplicaSlot(int replicas, long baseCounter)
    {
        var dots = new List<OrSetDot>(replicas);
        for (var i = 0; i < replicas; i++)
        {
            dots.Add(new OrSetDot { ReplicaId = "replica-" + i.ToString(CultureInfo.InvariantCulture), Counter = baseCounter + i });
        }

        return dots;
    }

    /// <summary>Every dot from one replica, so compaction must collapse to one.</summary>
    private static List<OrSetDot> MakeSupersededSlot(int count)
    {
        var dots = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
        {
            dots.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
        }

        return dots;
    }

    /// <summary>More distinct replicas than the in-place scan handles, forcing the dictionary tail.</summary>
    private static List<OrSetDot> MakeManyReplicaSlot(int replicas)
    {
        var dots = MakeMultiReplicaSlot(replicas, baseCounter: 1);

        // A second, higher assertion from each replica so the tail has work.
        for (var i = 0; i < replicas; i++)
        {
            dots.Add(new OrSetDot { ReplicaId = "replica-" + i.ToString(CultureInfo.InvariantCulture), Counter = 1000 + i });
        }

        return dots;
    }

    private static OrSet MakeOrSet(int addsPerElement, int tombstonesPerElement, string addReplica, string tombReplica)
    {
        var set = new OrSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            var adds = new List<OrSetDot>(addsPerElement);
            for (var i = 0; i < addsPerElement; i++)
                adds.Add(new OrSetDot { ReplicaId = addReplica, Counter = i + 1 });

            var tombs = new List<OrSetDot>(tombstonesPerElement);
            for (var i = 0; i < tombstonesPerElement; i++)
                tombs.Add(new OrSetDot { ReplicaId = tombReplica, Counter = i + 1 });

            set.Adds[key] = adds;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    /// <summary>
    /// A live dot on one replica whose counter equals a tombstoned counter on
    /// another: the one shape a counter-only liveness test gets wrong.
    /// </summary>
    private static OrSet MakeCounterCollisionOrSet()
    {
        var set = new OrSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }];
            set.Tombstones[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 5 }];
        }

        return set;
    }

    private static RwFlag MakeRwFlag(int enables, int disables)
    {
        var flag = new RwFlag();
        for (var i = 0; i < enables; i++)
            flag.Enables.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
        for (var i = 0; i < disables; i++)
            flag.Disables.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
        for (var i = 0; i < disables; i++)
            flag.Tombstones.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
        return flag;
    }

    private static RwFlag MakeCounterCollisionRwFlag()
    {
        var flag = new RwFlag();
        flag.Enables.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = 5 });
        flag.Disables.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = 5 });
        flag.Tombstones.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = 5 });
        return flag;
    }

    /// <summary>
    /// Already at the bounded normal form - one dot per replica per element -
    /// which is what every mutation and merge re-runs <c>Compact</c> over.
    /// </summary>
    private static OrSet MakeNormalFormOrSet()
    {
        var set = new OrSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 9 }, new OrSetDot { ReplicaId = ReplicaB, Counter = 4 }];
            set.Tombstones[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 3 }];
        }

        return set;
    }

    private static RwSet MakeNormalFormRwSet()
    {
        var set = new RwSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 9 }, new OrSetDot { ReplicaId = ReplicaB, Counter = 4 }];
            set.Removes[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 6 }];
            set.Tombstones[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 3 }];
        }

        return set;
    }

    private static string ElementKey(int index) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes("element-payload-" + index.ToString(CultureInfo.InvariantCulture)));

    // ---------------------------------------------------------------------
    // Equivalence assertions.
    // ---------------------------------------------------------------------

    private static void AssertScanEquivalent(List<OrSetDot>[] dotSlots, List<OrSetDot>[] coverSlots, string label)
    {
        for (var i = 0; i < dotSlots.Length; i++)
        {
            var dots = dotSlots[i];
            var cover = coverSlots[i % coverSlots.Length];
            for (var d = 0; d < dots.Count; d++)
            {
                var dot = dots[d];
                if (CoversByIndexer(cover, in dot) != OrSetDotCompaction.Covers(cover, in dot))
                {
                    throw new InvalidOperationException($"Covers lanes disagree ({label}).");
                }
            }
        }
    }

    private static void AssertCompactScanEquivalent(List<OrSetDot> source, string label)
    {
        List<OrSetDot> baseline = [.. source];
        List<OrSetDot> optimized = [.. source];
        var baselineChanged = CompactMaxPerReplicaByIndexer(baseline);
        var optimizedChanged = OrSetDotCompaction.CompactMaxPerReplica(optimized);

        if (baselineChanged != optimizedChanged || !ListsEqual(baseline, optimized))
        {
            throw new InvalidOperationException($"Compaction scan lanes disagree ({label}).");
        }
    }

    private static void AssertLivenessEquivalent(OrSet set, string label)
    {
        if (CountOrSetByCounting(set) != set.Count)
            throw new InvalidOperationException($"Liveness lanes disagree ({label}).");
    }

    private static void AssertFlagEquivalent(RwFlag flag, string label)
    {
        var baseline = flag.Enables.Count > 0 && OrSetDotCompaction.CountLive(flag.Disables, flag.Tombstones) == 0;
        if (baseline != flag.IsEnabled)
            throw new InvalidOperationException($"Flag liveness lanes disagree ({label}).");
    }

    private static void AssertOrSetCompactionEquivalent(OrSet source, string label)
    {
        var baseline = CloneOrSet(source);
        var optimized = CloneOrSet(source);
        CompactOrSetPaired(baseline);
        optimized.Compact();

        if (!MapsEqual(baseline.Adds, optimized.Adds) || !MapsEqual(baseline.Tombstones, optimized.Tombstones))
            throw new InvalidOperationException($"OrSet compaction lanes disagree ({label}).");
    }

    private static void AssertRwSetCompactionEquivalent(RwSet source, string label)
    {
        var baseline = CloneRwSet(source);
        var optimized = CloneRwSet(source);
        CompactRwSetPaired(baseline);
        optimized.Compact();

        if (!MapsEqual(baseline.Adds, optimized.Adds)
            || !MapsEqual(baseline.Removes, optimized.Removes)
            || !MapsEqual(baseline.Tombstones, optimized.Tombstones))
        {
            throw new InvalidOperationException($"RwSet compaction lanes disagree ({label}).");
        }
    }

    private static OrSet CloneOrSet(OrSet source)
    {
        var clone = new OrSet();
        foreach (var (key, dots) in source.Adds) clone.Adds[key] = [.. dots];
        foreach (var (key, dots) in source.Tombstones) clone.Tombstones[key] = [.. dots];
        return clone;
    }

    private static RwSet CloneRwSet(RwSet source)
    {
        var clone = new RwSet();
        foreach (var (key, dots) in source.Adds) clone.Adds[key] = [.. dots];
        foreach (var (key, dots) in source.Removes) clone.Removes[key] = [.. dots];
        foreach (var (key, dots) in source.Tombstones) clone.Tombstones[key] = [.. dots];
        return clone;
    }

    private static bool ListsEqual(List<OrSetDot> left, List<OrSetDot> right)
    {
        if (left.Count != right.Count) return false;
        for (var i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i])) return false;
        }

        return true;
    }

    private static bool MapsEqual(Dictionary<string, List<OrSetDot>> left, Dictionary<string, List<OrSetDot>> right)
    {
        if (left.Count != right.Count) return false;
        foreach (var (key, leftDots) in left)
        {
            if (!right.TryGetValue(key, out var rightDots) || !ListsEqual(leftDots, rightDots)) return false;
        }

        return true;
    }
}
