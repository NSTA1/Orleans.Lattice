using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three quadratic dot-list tests the observed-remove and remove-wins
/// set provenance decoders paid per element, so their time and byte deltas are
/// measurable in the clear rather than buried under a silo, a grain call and a
/// transport.
/// <para>
/// (1) <b>OR-set live-member projection.</b> A churned element accumulates
/// tombstones, and each of its add dots was tested for cancellation by linear
/// scan over that whole list, so the projection cost O(adds x tombstones) per
/// element. Cancellation here is <i>coverage</i>-based - a dot is cancelled
/// when the same replica tombstoned any counter at or above it - so when an
/// element's tombstones all carry one replica id the whole list collapses to
/// that replica's highest counter and the test becomes one comparison. Unlike
/// the OR-map trim this needs no sorted counter set at all, only a maximum, so
/// the indexed lane allocates nothing.
/// </para>
/// <para>
/// (2) <b>OR-set folded-state decode.</b> Each tombstone asks whether the add
/// list still holds its exact dot, to decide whether to synthesize the Added
/// half of a compacted-away add. That is exact containment rather than
/// coverage, so the shared-replica precondition reduces it to a counter lookup
/// served by sorting the add counters into a pooled buffer once and
/// binary-searching it - the OR-map pattern applied verbatim.
/// </para>
/// <para>
/// (3) <b>RW-set live-member projection.</b> The remove-wins exclusion test
/// walked every remove dot against the element's whole observed-add tombstone
/// list, counting survivors. The same coverage collapse applies, and the caller
/// only asks whether any remove survives, so the walk now stops at the first.
/// </para>
/// <para>
/// Read every group for <b>time</b>. Groups (1) and (3) remove comparisons
/// rather than heap traffic, so their byte columns are expected to be identical
/// and a claimed byte win there would be noise; group (2) rents its scratch
/// from <c>ArrayPool</c>, so it should be a byte wash too. Each group is swept
/// at both a churned and an un-churned shape, so the control shows the
/// threshold does not tax the common case. Every baseline lane is asserted in
/// <see cref="Setup"/> to produce exactly the answer its shipped counterpart
/// produces - including over a multi-replica shape, where the index must not be
/// taken, and a counter-collision shape, which is the one case a counter-only
/// test would get wrong if the replica guard were dropped. A lane that answers
/// differently is measuring different work, and the comparison would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=orsetdecodetrims</c> (or
/// <c>--suite orsetdecodetrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDotCoverageCollapseBenchmarks
{
    private const int ElementCount = 64;

    // The churned shape the trims exist for: the surviving dots sit at the end
    // of the add list and the tombstones cancel everything before them, which
    // is the scan's worst case and the shape churn actually produces.
    private const int ChurnedAddsPerElement = 24;
    private const int ChurnedTombstonesPerElement = 20;

    // The un-churned control, below the threshold, where the shipped path keeps
    // the linear scan and the two lanes must agree.
    private const int QuietAddsPerElement = 3;
    private const int QuietTombstonesPerElement = 2;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    private OrSet _churnedSet = null!;
    private OrSet _quietSet = null!;
    private RwSet _churnedRwSet = null!;
    private RwSet _quietRwSet = null!;
    private OrFlag[] _churnedFlags = null!;
    private OrFlag[] _quietFlags = null!;

    /// <summary>Builds the sets the lanes decode and pins every baseline to its shipped counterpart.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _churnedSet = MakeOrSet(ChurnedAddsPerElement, ChurnedTombstonesPerElement);
        _quietSet = MakeOrSet(QuietAddsPerElement, QuietTombstonesPerElement);
        _churnedRwSet = MakeRwSet(ChurnedTombstonesPerElement, ChurnedTombstonesPerElement);
        _quietRwSet = MakeRwSet(QuietTombstonesPerElement, QuietTombstonesPerElement);
        _churnedFlags = MakeFlags(ChurnedAddsPerElement, ChurnedTombstonesPerElement);
        _quietFlags = MakeFlags(QuietAddsPerElement, QuietTombstonesPerElement);

        AssertCurrentValueEquivalent(_churnedSet, "or-set churned projection");
        AssertCurrentValueEquivalent(_quietSet, "or-set quiet projection");
        AssertCurrentValueEquivalent(MakeMultiReplicaOrSet(), "or-set multi-replica tombstones");
        AssertCurrentValueEquivalent(MakeCounterCollisionOrSet(), "or-set counter collision across replicas");


        AssertCountEquivalent(_churnedSet, "or-set churned count");
        AssertCountEquivalent(_quietSet, "or-set quiet count");
        AssertCountEquivalent(MakeMultiReplicaOrSet(), "or-set multi-replica count");
        AssertCountEquivalent(MakeCounterCollisionOrSet(), "or-set counter-collision count");

        AssertFlagsEquivalent(_churnedFlags, "or-flag churned liveness");
        AssertFlagsEquivalent(_quietFlags, "or-flag quiet liveness");
        AssertFlagsEquivalent(MakeMultiReplicaFlags(), "or-flag multi-replica tombstones");
        AssertFlagsEquivalent(MakeCounterCollisionFlags(), "or-flag counter collision across replicas");

        AssertRwCurrentValueEquivalent(_churnedRwSet, "rw-set churned projection");
        AssertRwCurrentValueEquivalent(_quietRwSet, "rw-set quiet projection");
        AssertRwCurrentValueEquivalent(MakeMultiReplicaRwSet(), "rw-set multi-replica tombstones");
        AssertRwCurrentValueEquivalent(MakeCounterCollisionRwSet(), "rw-set counter collision across replicas");
    }

    // ========================================================================
    // (1) OR-set live-member projection
    // ========================================================================

    /// <summary>
    /// The prior body on a <b>churned</b> set: every add dot tested by linear
    /// coverage scan over the element's whole tombstone list.
    /// </summary>
    [Benchmark]
    public int OrSetCurrentValueChurned_Baseline_LinearScan()
        => ProjectOrSetLinearScan(_churnedSet).Count;

    /// <summary>
    /// The shipped shape through the <b>real production</b> decoder: above the
    /// threshold a single-replica tombstone list collapses to its highest
    /// counter.
    /// </summary>
    [Benchmark]
    public int OrSetCurrentValueChurned_Optimized_Indexed()
        => OrSetProvenanceDecoder.Instance.DecodeCurrentValue(_churnedSet).Count;

    /// <summary>The below-threshold control, where the shipped path keeps the scan.</summary>
    [Benchmark]
    public int OrSetCurrentValueQuiet_Baseline_LinearScan()
        => ProjectOrSetLinearScan(_quietSet).Count;

    /// <summary>The shipped path on the un-churned control.</summary>
    [Benchmark]
    public int OrSetCurrentValueQuiet_Optimized_Indexed()
        => OrSetProvenanceDecoder.Instance.DecodeCurrentValue(_quietSet).Count;

    // ========================================================================
    // (2) shared liveness reads (OrSet.Count / OrFlag.IsEnabled)
    // ========================================================================

    /// <summary>
    /// The prior body on a <b>churned</b> set: every add dot tested by linear
    /// coverage scan over the element's whole tombstone list, for every
    /// element, which is what a whole-set <c>Count</c> read pays.
    /// </summary>
    [Benchmark]
    public int OrSetCountChurned_Baseline_LinearScan()
        => CountOrSetLinearScan(_churnedSet);

    /// <summary>
    /// The shipped shape through the <b>real production</b> property: above the
    /// threshold a single-replica tombstone list collapses to its highest
    /// counter once per element.
    /// </summary>
    [Benchmark]
    public int OrSetCountChurned_Optimized_Collapsed() => _churnedSet.Count;

    /// <summary>The below-threshold control, where the shipped path keeps the scan.</summary>
    [Benchmark]
    public int OrSetCountQuiet_Baseline_LinearScan()
        => CountOrSetLinearScan(_quietSet);

    /// <summary>The shipped path on the un-churned control.</summary>
    [Benchmark]
    public int OrSetCountQuiet_Optimized_Collapsed() => _quietSet.Count;

    /// <summary>
    /// The prior body of the any-survivor read on <b>churned</b> flags, which
    /// is the enable-wins flag's whole liveness decision.
    /// </summary>
    [Benchmark]
    public int OrFlagEnabledChurned_Baseline_LinearScan()
        => CountEnabledLinearScan(_churnedFlags);

    /// <summary>The shipped shape through the <b>real production</b> property.</summary>
    [Benchmark]
    public int OrFlagEnabledChurned_Optimized_Collapsed()
        => CountEnabled(_churnedFlags);

    /// <summary>The below-threshold control, where the shipped path keeps the scan.</summary>
    [Benchmark]
    public int OrFlagEnabledQuiet_Baseline_LinearScan()
        => CountEnabledLinearScan(_quietFlags);

    /// <summary>The shipped path on the un-churned control.</summary>
    [Benchmark]
    public int OrFlagEnabledQuiet_Optimized_Collapsed()
        => CountEnabled(_quietFlags);

    // ========================================================================
    // (3) RW-set live-member projection
    // ========================================================================

    /// <summary>
    /// The prior body on a <b>churned</b> set: every remove dot tested by
    /// linear coverage scan, and every survivor counted rather than the walk
    /// stopping at the first.
    /// </summary>
    [Benchmark]
    public int RwSetCurrentValueChurned_Baseline_LinearScan()
        => ProjectRwSetLinearScan(_churnedRwSet).Count;

    /// <summary>The shipped shape through the <b>real production</b> decoder.</summary>
    [Benchmark]
    public int RwSetCurrentValueChurned_Optimized_Indexed()
        => RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_churnedRwSet).Count;

    /// <summary>The below-threshold control, where the shipped path keeps the scan.</summary>
    [Benchmark]
    public int RwSetCurrentValueQuiet_Baseline_LinearScan()
        => ProjectRwSetLinearScan(_quietRwSet).Count;

    /// <summary>The shipped path on the un-churned control.</summary>
    [Benchmark]
    public int RwSetCurrentValueQuiet_Optimized_Indexed()
        => RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_quietRwSet).Count;

    // ========================================================================
    // baseline reproductions
    //
    // Each reproduces the prior body behind exactly the dispatch production
    // pays - the public entry point's null check and its cast from object - so
    // the pair isolates the dot-list test and nothing else. The equivalence
    // assertions in Setup pin each reproduction to the shipped output member
    // for member.
    // ========================================================================

    private static IReadOnlyList<CrdtMemberValue> ProjectOrSetLinearScan(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (OrSet)state;
        var adds = set.Adds;
        if (adds.Count == 0) return Array.Empty<CrdtMemberValue>();

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(StringComparer.Ordinal);

        var tombstones = set.Tombstones;
        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            tombstones.TryGetValue(key, out var tomb);

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            for (var i = 0; i < addDots.Count; i++)
            {
                var dot = addDots[i];
                if (CoversLinear(tomb, in dot)) continue;
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result.Count == 0 ? Array.Empty<CrdtMemberValue>() : result;
    }

    private static IReadOnlyList<CrdtMemberValue> ProjectRwSetLinearScan(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (RwSet)state;
        var adds = set.Adds;
        if (adds.Count == 0) return Array.Empty<CrdtMemberValue>();

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            if (addDots.Count == 0) continue;
            if (LiveRemoveCountLinear(set, key) != 0) continue;

            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            var hasLive = false;
            for (var i = 0; i < addDots.Count; i++)
            {
                var dot = addDots[i];
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result.Count == 0 ? Array.Empty<CrdtMemberValue>() : result;
    }

    private static int LiveRemoveCountLinear(RwSet set, string key)
    {
        if (!set.Removes.TryGetValue(key, out var removeDots) || removeDots.Count == 0) return 0;
        set.Tombstones.TryGetValue(key, out var tomb);
        if (tomb is null || tomb.Count == 0) return removeDots.Count;
        var live = 0;
        for (var i = 0; i < removeDots.Count; i++)
        {
            var dot = removeDots[i];
            if (!CoversLinear(tomb, in dot)) live++;
        }

        return live;
    }

    // The coverage predicate, reproduced here because the production one is
    // internal to the core library.
    private static bool CoversLinear(List<OrSetDot>? cover, in OrSetDot dot)
    {
        if (cover is null) return false;
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

    // ========================================================================
    // corpora
    // ========================================================================

    private static string ElementKey(int index)
        => Convert.ToBase64String(
            Encoding.UTF8.GetBytes(string.Create(CultureInfo.InvariantCulture, $"entity/{index:D6}/member")));

    private static OrSet MakeOrSet(int addsPerElement, int tombstonesPerElement)
    {
        var set = new OrSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            var adds = new List<OrSetDot>(addsPerElement);
            for (var i = 0; i < addsPerElement; i++)
            {
                adds.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
            }

            // The cancelled dots are the oldest, so the survivor is always found
            // last - the scan's worst case.
            var tombs = new List<OrSetDot>(tombstonesPerElement);
            for (var i = 0; i < tombstonesPerElement; i++)
            {
                tombs.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
            }

            set.Adds[key] = adds;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    // Tombstones spanning two replicas, so the shared-replica precondition
    // fails and the index must not be taken.
    private static OrSet MakeMultiReplicaOrSet()
    {
        var set = new OrSet();
        for (var k = 0; k < 8; k++)
        {
            var key = ElementKey(k);
            var adds = new List<OrSetDot>();
            var tombs = new List<OrSetDot>();
            for (var i = 0; i < ChurnedAddsPerElement; i++)
            {
                adds.Add(new OrSetDot { ReplicaId = (i % 2) == 0 ? ReplicaA : ReplicaB, Counter = i + 1 });
            }

            for (var i = 0; i < ChurnedTombstonesPerElement; i++)
            {
                tombs.Add(new OrSetDot { ReplicaId = (i % 2) == 0 ? ReplicaA : ReplicaB, Counter = i + 1 });
            }

            set.Adds[key] = adds;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    // Every tombstone carries one replica - so the index IS taken - while a
    // live dot on a different replica shares a tombstoned counter. A
    // counter-only test without the replica guard would wrongly cancel it.
    private static OrSet MakeCounterCollisionOrSet()
    {
        var set = new OrSet();
        for (var k = 0; k < 8; k++)
        {
            var key = ElementKey(k);
            var adds = new List<OrSetDot>();
            var tombs = new List<OrSetDot>();
            for (var i = 0; i < ChurnedTombstonesPerElement; i++)
            {
                adds.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
                tombs.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
            }

            adds.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = 1 });
            set.Adds[key] = adds;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    // Every element survives: each remove dot is cancelled by a tombstone on
    // the same replica. That is the worst case for the remove-wins test, since
    // the walk cannot stop early, so the lane measures the coverage collapse
    // rather than the early exit.
    private static RwSet MakeRwSet(int removesPerElement, int tombstonesPerElement)
    {
        var set = new RwSet();
        for (var k = 0; k < ElementCount; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = new List<OrSetDot>
            {
                new() { ReplicaId = ReplicaA, Counter = 1 },
                new() { ReplicaId = ReplicaA, Counter = 2 },
            };

            var removes = new List<OrSetDot>(removesPerElement);
            var tombs = new List<OrSetDot>(tombstonesPerElement);
            for (var i = 0; i < removesPerElement; i++)
            {
                removes.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = i + 1 });
            }

            for (var i = 0; i < tombstonesPerElement; i++)
            {
                tombs.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = i + 1 });
            }

            set.Removes[key] = removes;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    private static RwSet MakeMultiReplicaRwSet()
    {
        var set = new RwSet();
        for (var k = 0; k < 8; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = new List<OrSetDot> { new() { ReplicaId = ReplicaA, Counter = 1 } };
            var removes = new List<OrSetDot>();
            var tombs = new List<OrSetDot>();
            for (var i = 0; i < ChurnedTombstonesPerElement; i++)
            {
                var replica = (i % 2) == 0 ? ReplicaA : ReplicaB;
                removes.Add(new OrSetDot { ReplicaId = replica, Counter = i + 1 });
                tombs.Add(new OrSetDot { ReplicaId = replica, Counter = i + 1 });
            }

            set.Removes[key] = removes;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    private static RwSet MakeCounterCollisionRwSet()
    {
        var set = new RwSet();
        for (var k = 0; k < 8; k++)
        {
            var key = ElementKey(k);
            set.Adds[key] = new List<OrSetDot> { new() { ReplicaId = ReplicaA, Counter = 1 } };
            var removes = new List<OrSetDot>();
            var tombs = new List<OrSetDot>();
            for (var i = 0; i < ChurnedTombstonesPerElement; i++)
            {
                removes.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = i + 1 });
                tombs.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = i + 1 });
            }

            // Same counter as a tombstoned dot, different replica: still a live
            // remove, so the element must stay excluded.
            removes.Add(new OrSetDot { ReplicaId = "replica-c", Counter = 1 });
            set.Removes[key] = removes;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    // ========================================================================
    // equivalence assertions
    // ========================================================================

    private static void AssertCurrentValueEquivalent(OrSet set, string lane)
        => AssertMembersEqual(
            ProjectOrSetLinearScan(set),
            OrSetProvenanceDecoder.Instance.DecodeCurrentValue(set),
            lane);

    private static void AssertRwCurrentValueEquivalent(RwSet set, string lane)
        => AssertMembersEqual(
            ProjectRwSetLinearScan(set),
            RwSetProvenanceDecoder.Instance.DecodeCurrentValue(set),
            lane);

    private static void AssertMembersEqual(
        IReadOnlyList<CrdtMemberValue> baseline,
        IReadOnlyList<CrdtMemberValue> shipped,
        string lane)
    {
        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline projected {baseline.Count} members, shipped projected {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (CompareBytes(baseline[i].Element, shipped[i].Element) != 0
                || !string.Equals(baseline[i].ReplicaId, shipped[i].ReplicaId, StringComparison.Ordinal)
                || baseline[i].Ordinal != shipped[i].Ordinal)
            {
                throw new InvalidOperationException($"[{lane}] member {i} differs between baseline and shipped.");
            }
        }
    }

    private static int CompareBytes(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }

        return a.Length.CompareTo(b.Length);
    }

    /// <summary>
    /// The prior body of the whole-set live-element count: every add dot tested
    /// by linear coverage scan over the element's tombstone list.
    /// </summary>
    private static int CountOrSetLinearScan(OrSet set)
    {
        ArgumentNullException.ThrowIfNull(set);
        var n = 0;
        foreach (var (key, dots) in set.Adds)
        {
            if (dots.Count == 0) continue;
            set.Tombstones.TryGetValue(key, out var tomb);
            var live = 0;
            for (var i = 0; i < dots.Count; i++)
            {
                var dot = dots[i];
                if (!CoversLinear(tomb, in dot)) live++;
            }

            if (live > 0) n++;
        }

        return n;
    }

    /// <summary>The shipped any-survivor read, through the real production property.</summary>
    private static int CountEnabled(OrFlag[] flags)
    {
        var n = 0;
        for (var i = 0; i < flags.Length; i++)
        {
            if (flags[i].IsEnabled) n++;
        }

        return n;
    }

    /// <summary>The prior body of that read: a linear coverage scan per enable dot.</summary>
    private static int CountEnabledLinearScan(OrFlag[] flags)
    {
        var n = 0;
        for (var i = 0; i < flags.Length; i++)
        {
            var flag = flags[i];
            var enables = flag.Enables;
            var tomb = flag.Tombstones;
            var live = false;
            for (var j = 0; j < enables.Count; j++)
            {
                var dot = enables[j];
                if (!CoversLinear(tomb, in dot))
                {
                    live = true;
                    break;
                }
            }

            if (live) n++;
        }

        return n;
    }

    private static OrFlag[] MakeFlags(int enablesPerFlag, int tombstonesPerFlag)
    {
        var flags = new OrFlag[ElementCount];
        for (var i = 0; i < ElementCount; i++)
        {
            var flag = new OrFlag();
            for (var t = 0; t < tombstonesPerFlag; t++) flag.Tombstones.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = t + 1 });
            for (var e = 0; e < enablesPerFlag; e++) flag.Enables.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = e + 1 });
            flags[i] = flag;
        }

        return flags;
    }

    /// <summary>Flags whose tombstones span two replicas, so the collapse must decline.</summary>
    private static OrFlag[] MakeMultiReplicaFlags()
    {
        var flags = MakeFlags(ChurnedAddsPerElement, ChurnedTombstonesPerElement);
        for (var i = 0; i < flags.Length; i++)
        {
            flags[i].Tombstones.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = 500 });
            flags[i].Enables.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = 501 });
        }

        return flags;
    }

    /// <summary>
    /// Flags carrying a live enable on replica B whose counter collides with a
    /// tombstoned counter on replica A - the case a counter-only test breaks.
    /// </summary>
    private static OrFlag[] MakeCounterCollisionFlags()
    {
        var flags = MakeFlags(ChurnedAddsPerElement, ChurnedTombstonesPerElement);
        for (var i = 0; i < flags.Length; i++)
        {
            flags[i].Enables.Clear();
            flags[i].Enables.Add(new OrSetDot { ReplicaId = ReplicaB, Counter = 1 });
            flags[i].Enables.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = 1 });
        }

        return flags;
    }

    private static void AssertCountEquivalent(OrSet set, string lane)
    {
        var baseline = CountOrSetLinearScan(set);
        var shipped = set.Count;
        if (baseline != shipped)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline counted {baseline} live elements, shipped counted {shipped}.");
        }
    }

    private static void AssertFlagsEquivalent(OrFlag[] flags, string lane)
    {
        var baseline = CountEnabledLinearScan(flags);
        var shipped = CountEnabled(flags);
        if (baseline != shipped)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline counted {baseline} enabled flags, shipped counted {shipped}.");
        }
    }
}