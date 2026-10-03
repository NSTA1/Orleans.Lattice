using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three trims on the CRDT read and merge paths, so their time and
/// byte deltas are measurable in the clear rather than buried under a silo, a
/// grain call and a transport.
/// <para>
/// (1) <b><c>OrMap</c> asks "is any entry live" rather than "how many are".</b>
/// <c>LiveEntryCount</c> counted every live entry for <c>IsEmpty</c>,
/// <c>Count</c>, <c>ContainsKey</c> and <c>Keys</c> - and all four consume it
/// only as <c>&gt; 0</c>. An any-query exits on the first live entry, which
/// turns the linear arm from <c>O(entries * tomb)</c> into <c>O(tomb)</c>, and
/// lets the wide arm answer from one cheap linear probe without allocating the
/// tombstone index at all. <c>OrSet.HasLiveDot</c> already ships this shape.
/// </para>
/// <para>
/// (2) <b><c>OrMap.MergeFrom</c> sizes its dedup strategy by the incoming side
/// alone.</b> Both folds gated on <c>existing.Count + incoming.Count</c>, so a
/// churned key carrying a long observed-remove history allocated a
/// <c>HashSet</c> (tombstones) or a <c>Dictionary</c> (adds) over its whole
/// accumulated list every time it absorbed even a one- or two-dot delta - an
/// index built to answer two probes. Only the incoming side has to be small for
/// the linear probe to stay bounded. <c>OrSet.MergeMap</c> and
/// <c>OrFlag.UnionInto</c> already carry the corrected gate.
/// </para>
/// <para>
/// (3) <b><c>GSet.Merge</c> and <c>GSet.Clone</c> copy their source set through
/// a comparer the source already carries.</b> Both built the result through a
/// comparer that is reference-distinct from a default-comparer source's, which
/// defeats <c>HashSet</c>'s copy-constructor bulk-copy path and re-hashes every
/// element - work the source set has already done and stored. <c>Merge</c> was
/// worse still: it filled an empty presized set by unioning <em>both</em>
/// operands in, so the left operand was re-hashed in full on every replicated
/// reconcile. <c>MvRegister.Clone</c> already documents and ships this fix for
/// its dot-context dictionary.
/// </para>
/// <para>
/// Read group 1 for <b>bytes and time</b> - the wide lane removes an allocation
/// outright and the narrow lane removes work. Read group 2 for <b>bytes</b>
/// first: it deletes an index allocation on the churned-key lane. Read group 3
/// for <b>time</b>; it trades a presized single allocation for a bulk copy plus
/// growth, so the <c>Allocated</c> column is reported precisely rather than
/// asserted.
/// </para>
/// <para>
/// Every group carries a control lane where the trim is expected to buy nothing
/// - a key whose entries are all tombstoned so no early exit can fire, a fold
/// whose two sides are both below the threshold, and a peer set disjoint from
/// and as wide as the local one - because a trim that taxes the degenerate case
/// to help the common one is not a trim. The group 3 disjoint-wide lane is the
/// arm that gives up the baseline's combined presize, so it is the check that
/// the growth path the trim falls back on stays cheaper than the left-side
/// rehash it removed. Group 2 additionally carries a lane
/// isolating the per-invocation slot copy both its arms pay, so the end-to-end
/// delta can be attributed rather than guessed at.
/// </para>
/// <para>
/// Every baseline lane is a verbatim copy of the code the trim replaced, with
/// the private thresholds it reads mirrored here, so it pays exactly the
/// dispatch its shipped counterpart pays. Each pair is asserted in
/// <see cref="Setup"/> to answer identically over corpora that include an empty
/// slot, a first-entry hit, a last-entry hit, a fully tombstoned key, a key
/// with no tombstones at all, a counter collision across replicas, a slot
/// that is its own merge source, and a set whose comparer is neither the
/// default nor ordinal. A lane that answers differently is measuring
/// different work and the comparison would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtlivenessgatetrims</c> (or
/// <c>--suite crdtlivenessgatetrims</c>); see <c>Program.cs</c>. No Orleans
/// silo is involved, so it runs cheaply at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtLivenessAndDedupGateTrimBenchmarks
{
    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";
    private const string ReplicaC = "replica-c";

    /// <summary>
    /// Mirrors the private <c>OrMap.LinearDedupThreshold</c>, so the baseline
    /// lanes in groups 1 and 2 pick the same arm their shipped counterparts do.
    /// </summary>
    private const int BaselineLinearDedupThreshold = 16;

    /// <summary>
    /// Mirrors the private <c>OrSet.DotLinearScanThreshold</c> (and the
    /// identical constant in <c>RwSet</c>), so the group 3 lanes pick the same
    /// arm their shipped counterparts do.
    /// </summary>
    private const int BaselineDotLinearScanThreshold = 4;

    // Group 1 - OrMap liveness probe. A churned key carries a long observed-
    // remove history and a handful of live entries; the "all dead" corpus is
    // the control where no early exit can fire.
    private const int LiveEntryCount = 32;
    private const int WideTombCount = 64;
    private const int NarrowTombCount = 4;

    private List<OrMapEntry<GSet>> _entriesFirstLive = null!;
    private List<OrMapEntry<GSet>> _entriesAllDead = null!;
    private List<OrSetDot> _wideTomb = null!;
    private List<OrSetDot> _narrowTomb = null!;

    // Group 2 - OrMap.MergeFrom dedup gate.
    private const int ChurnedExistingCount = 64;
    private const int SmallExistingCount = 4;

    private List<OrSetDot> _churnedExisting = null!;
    private List<OrSetDot> _smallExisting = null!;
    private List<OrSetDot> _tinyIncoming = null!;
    private List<OrSetDot> _wideIncoming = null!;

    // Group 3 - GSet merge / clone element rehash.
    private const int GSetLocalCount = 256;
    private const int BaselineCopyMergeWidthRatio = 4;

    private HashSet<string> _gsetLocal = null!;
    private HashSet<string> _gsetSubsetPeer = null!;
    private HashSet<string> _gsetDisjointPeer = null!;

    /// <summary>
    /// Builds every corpus and asserts each optimised lane answers exactly what
    /// its verbatim baseline answers, including over inputs that violate the
    /// precondition each trim relies on.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _wideTomb = MakeDots(WideTombCount, baseCounter: 1);
        _narrowTomb = MakeDots(NarrowTombCount, baseCounter: 1);

        // The first entry carries a dot that is in neither tombstone list, so
        // an any-query exits immediately; every later entry is tombstoned, so
        // a counting walk still has to visit all of them.
        _entriesFirstLive = MakeEntries(LiveEntryCount, firstLive: true);
        _entriesAllDead = MakeEntries(LiveEntryCount, firstLive: false);

        _churnedExisting = MakeDots(ChurnedExistingCount, baseCounter: 1);
        _smallExisting = MakeDots(SmallExistingCount, baseCounter: 1);
        // Half hit, half miss, so neither arm is measured purely on its
        // early-exit path.
        _tinyIncoming = MakeDots(2, baseCounter: ChurnedExistingCount);
        _wideIncoming = MakeDots(32, baseCounter: ChurnedExistingCount);

        // Built through GSet's own parameterless constructor, which is what a
        // freshly-authored or newly-deserialized set carries: the default string
        // comparer, reference-distinct from StringComparer.Ordinal and therefore
        // the exact shape that defeated the copy constructor's bulk-copy path.
        _gsetLocal = MakeElements(GSetLocalCount, baseIndex: 0);
        _gsetSubsetPeer = MakeElements(16, baseIndex: 0);
        _gsetDisjointPeer = MakeElements(GSetLocalCount, baseIndex: GSetLocalCount);

        AssertLivenessEquivalent(_entriesFirstLive, _wideTomb, "first-live/wide-tomb");
        AssertLivenessEquivalent(_entriesFirstLive, _narrowTomb, "first-live/narrow-tomb");
        AssertLivenessEquivalent(_entriesAllDead, _wideTomb, "all-dead/wide-tomb");
        AssertLivenessEquivalent(_entriesAllDead, _narrowTomb, "all-dead/narrow-tomb");
        AssertLivenessEquivalent(_entriesFirstLive, null, "first-live/no-tomb");
        AssertLivenessEquivalent([], _wideTomb, "empty-entries/wide-tomb");
        AssertLivenessEquivalent([], null, "empty-entries/no-tomb");
        AssertLivenessEquivalent(MakeEntries(1, firstLive: false), _wideTomb, "single-dead-entry");
        AssertLivenessEquivalent(MakeLastLiveEntries(LiveEntryCount), _wideTomb, "last-live/wide-tomb");
        AssertLivenessEquivalent(MakeLastLiveEntries(LiveEntryCount), _narrowTomb, "last-live/narrow-tomb");

        AssertTombFoldEquivalent(_churnedExisting, _tinyIncoming, "churned/tiny-incoming");
        AssertTombFoldEquivalent(_churnedExisting, _wideIncoming, "churned/wide-incoming");
        AssertTombFoldEquivalent(_smallExisting, _tinyIncoming, "small/tiny-incoming");
        AssertTombFoldEquivalent([], _tinyIncoming, "empty-existing");
        AssertTombFoldEquivalent(_churnedExisting, [], "empty-incoming");
        AssertTombFoldEquivalent(_churnedExisting, _churnedExisting, "self-fold");
        AssertTombFoldEquivalent(MakeCounterCollisionDots(), _tinyIncoming, "counter-collision");

        AssertGSetMergeEquivalent(_gsetLocal, _gsetSubsetPeer, "gset/subset-peer");
        AssertGSetMergeEquivalent(_gsetLocal, _gsetDisjointPeer, "gset/disjoint-peer");
        AssertGSetMergeEquivalent(_gsetLocal, [], "gset/empty-peer");
        AssertGSetMergeEquivalent([], _gsetSubsetPeer, "gset/empty-local");
        AssertGSetMergeEquivalent(_gsetLocal, _gsetLocal, "gset/self-merge");
        // Precondition-violating corpus: a local set whose comparer is neither
        // the default nor ordinal. The trim must still normalise it, so the two
        // sides must agree on membership even though the comparers differ.
        AssertGSetMergeEquivalent(
            new HashSet<string>(_gsetSubsetPeer, StringComparer.OrdinalIgnoreCase),
            _gsetDisjointPeer,
            "gset/non-ordinal-local");
    }

    // ---------------------------------------------------------------- group 1

    /// <summary>Churned key, long tombstone history: the baseline builds the tombstone index.</summary>
    [Benchmark]
    public bool LivenessWideTomb_Baseline_CountAll() =>
        LiveEntryCountBaseline(_entriesFirstLive, _wideTomb) > 0;

    /// <summary>Churned key, long tombstone history: one linear probe answers it allocation-free.</summary>
    [Benchmark]
    public bool LivenessWideTomb_Optimized_AnyEarlyExit() =>
        HasLiveEntryOptimized(_entriesFirstLive, _wideTomb);

    /// <summary>Short tombstone list: the linear arm, where the early exit removes the entry walk.</summary>
    [Benchmark]
    public bool LivenessNarrowTomb_Baseline_CountAll() =>
        LiveEntryCountBaseline(_entriesFirstLive, _narrowTomb) > 0;

    /// <summary>Short tombstone list: exits on the first live entry.</summary>
    [Benchmark]
    public bool LivenessNarrowTomb_Optimized_AnyEarlyExit() =>
        HasLiveEntryOptimized(_entriesFirstLive, _narrowTomb);

    /// <summary>Control: every entry is tombstoned, so no early exit can fire and both arms do identical work.</summary>
    [Benchmark]
    public bool LivenessAllDead_Baseline_CountAll() =>
        LiveEntryCountBaseline(_entriesAllDead, _wideTomb) > 0;

    /// <summary>Control twin of <see cref="LivenessAllDead_Baseline_CountAll"/>.</summary>
    [Benchmark]
    public bool LivenessAllDead_Optimized_AnyEarlyExit() =>
        HasLiveEntryOptimized(_entriesAllDead, _wideTomb);

    /// <summary>Control: no tombstones at all, the arm both shapes answer from the entry count.</summary>
    [Benchmark]
    public bool LivenessNoTomb_Baseline_CountAll() =>
        LiveEntryCountBaseline(_entriesFirstLive, null) > 0;

    /// <summary>Control twin of <see cref="LivenessNoTomb_Baseline_CountAll"/>.</summary>
    [Benchmark]
    public bool LivenessNoTomb_Optimized_AnyEarlyExit() =>
        HasLiveEntryOptimized(_entriesFirstLive, null);

    // ---------------------------------------------------------------- group 2

    /// <summary>Churned key absorbing a two-dot delta: the sum gate builds an index over the whole history.</summary>
    [Benchmark]
    public int TombFoldChurned_Baseline_SumGate()
    {
        var target = new List<OrSetDot>(_churnedExisting);
        TombFoldBaseline(target, _tinyIncoming);
        return target.Count;
    }

    /// <summary>Churned key absorbing a two-dot delta: the incoming gate keeps it allocation-free.</summary>
    [Benchmark]
    public int TombFoldChurned_Optimized_IncomingGate()
    {
        var target = new List<OrSetDot>(_churnedExisting);
        TombFoldOptimized(target, _tinyIncoming);
        return target.Count;
    }

    /// <summary>Control: a wide incoming delta, where both gates reach the indexed arm.</summary>
    [Benchmark]
    public int TombFoldWideIncoming_Baseline_SumGate()
    {
        var target = new List<OrSetDot>(_churnedExisting);
        TombFoldBaseline(target, _wideIncoming);
        return target.Count;
    }

    /// <summary>Control twin of <see cref="TombFoldWideIncoming_Baseline_SumGate"/>.</summary>
    [Benchmark]
    public int TombFoldWideIncoming_Optimized_IncomingGate()
    {
        var target = new List<OrSetDot>(_churnedExisting);
        TombFoldOptimized(target, _wideIncoming);
        return target.Count;
    }

    /// <summary>Control: both sides below the threshold, where both gates reach the linear arm.</summary>
    [Benchmark]
    public int TombFoldSmallBoth_Baseline_SumGate()
    {
        var target = new List<OrSetDot>(_smallExisting);
        TombFoldBaseline(target, _tinyIncoming);
        return target.Count;
    }

    /// <summary>Control twin of <see cref="TombFoldSmallBoth_Baseline_SumGate"/>.</summary>
    [Benchmark]
    public int TombFoldSmallBoth_Optimized_IncomingGate()
    {
        var target = new List<OrSetDot>(_smallExisting);
        TombFoldOptimized(target, _tinyIncoming);
        return target.Count;
    }

    /// <summary>Isolates the per-invocation slot copy both group 2 arms pay, so their delta can be attributed.</summary>
    [Benchmark]
    public int TombFoldCopyOnly_Control() => new List<OrSetDot>(_churnedExisting).Count;

    // ---------------------------------------------------------------- group 3

    /// <summary>
    /// Steady-state replicated merge: a peer set whose elements this replica has
    /// already observed, folded into the local set. Idempotent delivery makes
    /// this the dominant merge shape. The baseline re-hashes every element of
    /// both operands into a presized union.
    /// </summary>
    [Benchmark]
    public int GSetMergeSubset_Baseline_PresizeUnionBoth()
        => MergeBaselinePresize(_gsetLocal, _gsetSubsetPeer).Count;

    /// <summary>The same merge seeded by the comparer-preserving copy constructor.</summary>
    [Benchmark]
    public int GSetMergeSubset_Optimized_CopyThenUnion()
        => MergeOptimizedCopy(_gsetLocal, _gsetSubsetPeer).Count;

    /// <summary>
    /// Control: a peer set disjoint from the local set and as wide as it. This
    /// is the arm that gives up the baseline's combined presize, so it is where
    /// the trim could regress - the growth it falls back on must stay cheaper
    /// than the left-side rehash it removed.
    /// </summary>
    [Benchmark]
    public int GSetMergeDisjointWide_Baseline_PresizeUnionBoth()
        => MergeBaselinePresize(_gsetLocal, _gsetDisjointPeer).Count;

    /// <summary>Control twin of <see cref="GSetMergeDisjointWide_Baseline_PresizeUnionBoth"/>.</summary>
    [Benchmark]
    public int GSetMergeDisjointWide_Optimized_CopyThenUnion()
        => MergeOptimizedCopy(_gsetLocal, _gsetDisjointPeer).Count;

    /// <summary>
    /// The clone every static <c>Merge</c> performs before folding. The baseline
    /// passes a comparer that is reference-distinct from the source set's, which
    /// defeats the copy constructor's bulk-copy path and re-hashes every
    /// element.
    /// </summary>
    [Benchmark]
    public int GSetClone_Baseline_FreshOrdinalComparer()
        => CloneBaselineFreshComparer(_gsetLocal).Count;

    /// <summary>The same clone through the source set's own ordinal comparer.</summary>
    [Benchmark]
    public int GSetClone_Optimized_PreserveComparer()
        => CloneOptimizedPreserveComparer(_gsetLocal).Count;

    // ------------------------------------------------------- baseline mirrors

    /// <summary>
    /// Verbatim copy of the <c>OrMap.LiveEntryCount</c> body this trim replaced,
    /// with the per-key tombstone lookup lifted into the parameter so both arms
    /// pay the same dictionary probe outside the measured region.
    /// </summary>
    private static int LiveEntryCountBaseline(List<OrMapEntry<GSet>> entries, List<OrSetDot>? tomb)
    {
        if (tomb is null || tomb.Count == 0) return entries.Count;

        if (tomb.Count <= BaselineLinearDedupThreshold || entries.Count <= BaselineLinearDedupThreshold)
        {
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDotMirror(tomb, dot)) n++;
            }
            return n;
        }

        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }

    /// <summary>Mirrors the private <c>OrMap.ListContainsDot</c> span scan the baseline arm calls.</summary>
    private static bool ListContainsDotMirror(List<OrSetDot> list, in OrSetDot dot)
    {
        var span = CollectionsMarshal.AsSpan(list);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var candidate = ref span[i];
            if (candidate.Counter == dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Mirrors the shipped <c>OrMap.HasLiveEntry</c> dispatcher.</summary>
    private static bool HasLiveEntryOptimized(List<OrMapEntry<GSet>> entries, List<OrSetDot>? tomb)
    {
        if (entries.Count == 0) return false;
        if (tomb is null || tomb.Count == 0) return true;

        if (tomb.Count <= BaselineLinearDedupThreshold || entries.Count <= BaselineLinearDedupThreshold)
        {
            return HasLiveEntryByScanMirror(entries, tomb);
        }

        return HasLiveEntryByIndexMirror(entries, tomb);
    }

    private static bool HasLiveEntryByScanMirror(List<OrMapEntry<GSet>> entries, List<OrSetDot> tomb)
    {
        foreach (var e in entries)
        {
            var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
            if (!ListContainsDotMirror(tomb, dot)) return true;
        }

        return false;
    }

    private static bool HasLiveEntryByIndexMirror(List<OrMapEntry<GSet>> entries, List<OrSetDot> tomb)
    {
        var first = entries[0];
        var probe = new OrSetDot { ReplicaId = first.ReplicaId, Counter = first.Counter };
        if (!ListContainsDotMirror(tomb, probe)) return true;

        var tombSet = OrSetDotSet.Build(tomb);
        for (var i = 1; i < entries.Count; i++)
        {
            var e = entries[i];
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) return true;
        }

        return false;
    }

    /// <summary>
    /// Verbatim copy of the <c>OrMap.MergeFrom</c> tombstone fold this trim
    /// replaced, gate included.
    /// </summary>
    private static void TombFoldBaseline(List<OrSetDot> existing, List<OrSetDot> dots)
    {
        if (existing.Count + dots.Count <= BaselineLinearDedupThreshold)
        {
            foreach (var d in dots)
            {
                if (!ListContainsDotMirror(existing, d)) existing.Add(d);
            }
            return;
        }

        var seen = new HashSet<OrSetDot>(existing.Count + dots.Count);
        foreach (var d in existing) seen.Add(d);
        foreach (var d in dots)
        {
            if (seen.Add(d)) existing.Add(d);
        }
    }

    /// <summary>Mirrors the shipped incoming-gated fold and its two sibling arms.</summary>
    private static void TombFoldOptimized(List<OrSetDot> existing, List<OrSetDot> dots)
    {
        if (ReferenceEquals(existing, dots)) return;
        if (dots.Count <= BaselineLinearDedupThreshold)
        {
            TombFoldByScanMirror(existing, dots);
            return;
        }

        TombFoldByIndexMirror(existing, dots);
    }

    private static void TombFoldByScanMirror(List<OrSetDot> existing, List<OrSetDot> dots)
    {
        var span = CollectionsMarshal.AsSpan(dots);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (!ListContainsDotMirror(existing, dot)) existing.Add(dot);
        }
    }

    private static void TombFoldByIndexMirror(List<OrSetDot> existing, List<OrSetDot> dots)
    {
        var seen = OrSetDotSet.Build(existing, dots.Count);
        var span = CollectionsMarshal.AsSpan(dots);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (seen.Add(dot)) existing.Add(dot);
        }
    }

    /// <summary>
    /// Verbatim copy of the <c>GSet.Merge</c> body this trim replaced: an empty
    /// union presized to the combined upper bound, filled by unioning both
    /// operands in - which hashes every element of the left operand even though
    /// the source set has already stored each element's hash code.
    /// </summary>
    private static HashSet<string> MergeBaselinePresize(HashSet<string> left, HashSet<string> right)
    {
        var union = new HashSet<string>(left.Count + right.Count, StringComparer.Ordinal);
        union.UnionWith(left);
        union.UnionWith(right);
        return union;
    }

    /// <summary>Mirrors the shipped width-gated merge dispatcher.</summary>
    private static HashSet<string> MergeOptimizedCopy(HashSet<string> left, HashSet<string> right)
        => right.Count * BaselineCopyMergeWidthRatio <= left.Count
            ? MergeByCopyThenUnionMirror(left, right)
            : MergeBaselinePresize(left, right);

    /// <summary>Mirrors the shipped narrow-right copy-then-union arm.</summary>
    private static HashSet<string> MergeByCopyThenUnionMirror(HashSet<string> left, HashSet<string> right)
    {
        var union = new HashSet<string>(left, OrdinalEquivalentMirror(left.Comparer));
        if (right.Count > 0) union.UnionWith(right);
        return union;
    }

    /// <summary>
    /// Verbatim copy of the <c>GSet.Clone</c> body this trim replaced: a copy
    /// through a comparer that is reference-distinct from a default-comparer
    /// source set's, which defeats the copy constructor's bulk-copy path.
    /// </summary>
    private static HashSet<string> CloneBaselineFreshComparer(HashSet<string> elements)
        => new(elements, StringComparer.Ordinal);

    /// <summary>Mirrors the shipped comparer-preserving clone.</summary>
    private static HashSet<string> CloneOptimizedPreserveComparer(HashSet<string> elements)
        => new(elements, OrdinalEquivalentMirror(elements.Comparer));

    /// <summary>Mirrors the shipped <c>GSet.OrdinalEquivalent</c> helper.</summary>
    private static IEqualityComparer<string> OrdinalEquivalentMirror(IEqualityComparer<string> sourceComparer)
        => ReferenceEquals(sourceComparer, EqualityComparer<string>.Default)
            ? sourceComparer
            : StringComparer.Ordinal;

    // ------------------------------------------------------------- corpus gen

    private static List<OrSetDot> MakeDots(int count, int baseCounter)
    {
        var dots = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
        {
            dots.Add(new OrSetDot
            {
                ReplicaId = (i % 3) switch { 0 => ReplicaA, 1 => ReplicaB, _ => ReplicaC },
                Counter = baseCounter + i,
            });
        }
        return dots;
    }

    /// <summary>
    /// A set of base64 element keys built through the default string comparer -
    /// the comparer <c>new GSet()</c> installs, and the shape the copy
    /// constructor's bulk-copy path was being denied.
    /// </summary>
    private static HashSet<string> MakeElements(int count, int baseIndex)
    {
        var elements = new HashSet<string>();
        for (var i = 0; i < count; i++)
        {
            elements.Add(Convert.ToBase64String(BitConverter.GetBytes((long)(baseIndex + i))));
        }
        return elements;
    }

    /// <summary>
    /// The same counter authored by three different replicas, so a probe that
    /// compared only the counter would answer differently from one that
    /// compares both members.
    /// </summary>
    private static List<OrSetDot> MakeCounterCollisionDots() =>
    [
        new OrSetDot { ReplicaId = ReplicaA, Counter = 7 },
        new OrSetDot { ReplicaId = ReplicaB, Counter = 7 },
        new OrSetDot { ReplicaId = ReplicaC, Counter = 7 },
    ];

    /// <summary>
    /// Entries whose dots are drawn from the tombstone corpora, except the
    /// first when <paramref name="firstLive"/> is set.
    /// </summary>
    private static List<OrMapEntry<GSet>> MakeEntries(int count, bool firstLive)
    {
        var entries = new List<OrMapEntry<GSet>>(count);
        for (var i = 0; i < count; i++)
        {
            var live = firstLive && i == 0;
            entries.Add(new OrMapEntry<GSet>(
                (i % 3) switch { 0 => ReplicaA, 1 => ReplicaB, _ => ReplicaC },
                live ? 1_000_000 + i : 1 + i,
                new GSet()));
        }
        return entries;
    }

    /// <summary>Only the final entry is live, so an any-query has to walk the whole list.</summary>
    private static List<OrMapEntry<GSet>> MakeLastLiveEntries(int count)
    {
        var entries = MakeEntries(count, firstLive: false);
        var last = entries[count - 1];
        entries[count - 1] = new OrMapEntry<GSet>(last.ReplicaId, 2_000_000, new GSet());
        return entries;
    }

    private static void AssertLivenessEquivalent(List<OrMapEntry<GSet>> entries, List<OrSetDot>? tomb, string label)
    {
        var expected = LiveEntryCountBaseline(entries, tomb) > 0;
        var actual = HasLiveEntryOptimized(entries, tomb);
        if (expected != actual)
        {
            throw new InvalidOperationException(
                $"liveness lanes disagree on '{label}': baseline={expected} optimized={actual}");
        }
    }

    private static void AssertTombFoldEquivalent(List<OrSetDot> existing, List<OrSetDot> dots, string label)
    {
        var baseline = new List<OrSetDot>(existing);
        TombFoldBaseline(baseline, dots);
        var optimized = new List<OrSetDot>(existing);
        TombFoldOptimized(optimized, dots);
        AssertSameSequence(baseline, optimized, $"tombstone fold '{label}'");
    }

    private static void AssertGSetMergeEquivalent(HashSet<string> left, HashSet<string> right, string label)
    {
        AssertSameMembership(
            MergeBaselinePresize(left, right),
            MergeOptimizedCopy(left, right),
            $"gset merge '{label}'");
        AssertSameMembership(
            CloneBaselineFreshComparer(left),
            CloneOptimizedPreserveComparer(left),
            $"gset clone '{label}'");
    }

    private static void AssertSameMembership(HashSet<string> baseline, HashSet<string> optimized, string label)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"{label}: baseline produced {baseline.Count} elements, optimized produced {optimized.Count}");
        }

        foreach (var element in baseline)
        {
            if (!optimized.Contains(element))
            {
                throw new InvalidOperationException($"{label}: optimized is missing '{element}'");
            }
        }
    }

    private static void AssertSameSequence(List<OrSetDot> baseline, List<OrSetDot> optimized, string label)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"{label}: baseline produced {baseline.Count} dots, optimized produced {optimized.Count}");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (!baseline[i].Equals(optimized[i]))
            {
                throw new InvalidOperationException($"{label}: dot {i} differs");
            }
        }
    }
}
