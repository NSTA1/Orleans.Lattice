using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three costs the OR-map provenance decoders and the view-projection
/// filter seam paid per item, so their time and byte deltas are measurable in
/// the clear rather than buried under a silo, a grain call, and a transport.
/// <para>
/// (1) <b>View-projection filter eligibility.</b> A view projection folds every
/// source mutation against one filter predicate, and choosing between the two
/// evaluators means walking that predicate tree to check that every member path
/// is a single top-level property name. The filter is fixed for the
/// projection's lifetime, so the walk is invariant not merely per call but per
/// instance - yet it ran once per mutation. The three projections now resolve
/// it in their constructor and pass it down, as does the atomic-write guard
/// loop, which folds a whole captured batch against one guard tree. These lanes
/// fold identical rows through the per-row and hoisted forms over a four-member
/// conjunction, the shape a real translated filter produces.
/// </para>
/// <para>
/// (2) <b>OR-map delta key surrogate per dot.</b> Decoding an author delta
/// rendered the key surrogate with a fresh UTF-8 encode, and a fresh array, for
/// every dot. A delta's dots arrive grouped by key - a single mutation ships
/// every dot it authored for one key together - so a one-slot memo over the
/// previous key collapses a group to one encode and one array. The state
/// emitter in the same type already hoists per key; this is the same hoist for
/// a flat, key-carrying list.
/// </para>
/// <para>
/// (3) <b>OR-map live-key projection tombstone test.</b> A key that has churned
/// accumulates tombstones, and each of its live dots was tested by linear scan
/// over that list, so the projection cost O(adds x tombstones) per key. Above a
/// threshold the scan is replaced by one pass into a reused hash set, making
/// the test O(adds + tombstones); below it the linear scan is kept, because
/// hashing a dot costs a string hash that over a handful of dots is dearer than
/// the counter-first comparison the scan runs.
/// </para>
/// <para>
/// Read group (1) for <b>time</b> and groups (2) and (3) for both: the first
/// removes a tree walk rather than heap traffic, so its byte column is expected
/// to be identical and a claimed byte win there would be noise. Group (3) is
/// swept at both a churned and an un-churned key shape, so the control shows
/// the threshold does not tax the common case. Every baseline lane is asserted
/// in <see cref="Setup"/> to produce exactly the answer its shipped counterpart
/// produces - a lane that answers differently is measuring different work, and
/// the comparison would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=ormapfilterhoisttrims</c> (or
/// <c>--suite ormapfilterhoisttrims</c>); see <c>Program.cs</c>. No Orleans
/// silo is involved, so it runs cheaply at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class OrMapDecodeAndFilterHoistTrimBenchmarks
{
    // One view-maintainer drain's worth of source mutations.
    private const int MutationRowCount = 256;

    // One shipped delta's dots: keys carrying several dots each, the shape a
    // multi-write mutation authors.
    private const int DeltaKeyCount = 64;
    private const int DotsPerKey = 4;

    // The churned-key shape group (3) exists for, and the un-churned control
    // that must stay a wash.
    private const int ProjectionKeyCount = 64;
    private const int ChurnedAddsPerKey = 24;
    private const int ChurnedTombstonesPerKey = 20;
    private const int QuietAddsPerKey = 3;
    private const int QuietTombstonesPerKey = 2;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    private byte[][] _rows = null!;
    private LatticePredicateNode _filter;
    private bool _filterEligible;

    private OrMapDelta<string, GCounter> _delta;
    private CrdtProvenanceDelta[] _deltaWrapper = null!;

    private OrMap<string, GCounter> _churnedMap = null!;
    private OrMap<string, GCounter> _quietMap = null!;

    /// <summary>Builds the rows, filter, delta and maps the lanes fold.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _rows = new byte[MutationRowCount][];
        for (var i = 0; i < MutationRowCount; i++)
        {
            _rows[i] = Encoding.UTF8.GetBytes(
                $"{{\"rank\":{i},\"name\":\"row-{i:D6}\",\"tag\":\"alpha\",\"active\":true,\"score\":{i * 3}}}");
        }

        // A four-member conjunction: the shape a translated `where` clause with
        // several terms produces, and the shape whose eligibility walk costs
        // enough per row to be worth hoisting.
        _filter = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.GreaterThanOrEqual,
                LatticePredicateNode.Member("rank"),
                LatticePredicateNode.Const(LatticeConstant.Integer(0))),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("tag"),
                LatticePredicateNode.Const(LatticeConstant.Text("alpha"))),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("active"),
                LatticePredicateNode.Const(LatticeConstant.Bool(true))),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.LessThan,
                LatticePredicateNode.Member("score"),
                LatticePredicateNode.Const(LatticeConstant.Integer(MutationRowCount * 3))));

        _filterEligible = LatticePredicateEvaluator.IsFastPathEligible(_filter);
        if (!_filterEligible)
        {
            throw new InvalidOperationException(
                "The benchmark filter must be fast-path eligible; otherwise the lanes measure the JsonDocument path.");
        }

        _delta = MakeGroupedDelta();
        _deltaWrapper = new[] { new CrdtProvenanceDelta(_delta, HybridLogicalClock.Zero) };

        _churnedMap = MakeMap(ChurnedAddsPerKey, ChurnedTombstonesPerKey);
        _quietMap = MakeMap(QuietAddsPerKey, QuietTombstonesPerKey);

        // A baseline lane is only honest evidence if it answers exactly what
        // the shipped lane answers. Assert that here rather than trusting the
        // reproductions by eye.
        AssertSame(
            FilterPerRow(),
            FilterHoisted(),
            "view-projection filter eligibility");

        AssertSame(
            EligibilityWalk_Baseline_PerRow(),
            EligibilityWalk_Optimized_Hoisted(),
            "eligibility walk");

        AssertDeltaEquivalent();
        AssertProjectionEquivalent(_churnedMap, "churned map");
        AssertProjectionEquivalent(_quietMap, "quiet map");

        // The counter index is licensed only when a key's tombstones all carry
        // one replica id. Assert the multi-replica shape, where the index must
        // not be taken, and the shape where a live dot shares a tombstoned
        // dot's counter under a different replica - the one case a
        // counter-only test would get wrong if the replica guard were dropped.
        AssertProjectionEquivalent(MakeMultiReplicaMap(), "multi-replica tombstones");
        AssertProjectionEquivalent(MakeCounterCollisionMap(), "counter collision across replicas");

        // The memo is only correct if it never carries a key's bytes into a
        // neighbouring key. Assert it over a deliberately interleaved delta
        // whose adjacent dots never share a key, which is the memo's worst
        // case and the one a grouping assumption would get wrong.
        AssertInterleavedDeltaEquivalent();
    }

    // ========================================================================
    // (1) view-projection filter eligibility
    // ========================================================================

    /// <summary>
    /// The prior shape: the parameterless <c>Matches</c>, which re-walks the
    /// whole filter tree on every mutation to rediscover an answer fixed at
    /// projection construction.
    /// </summary>
    [Benchmark]
    public int FilterEligibility_Baseline_PerRow() => FilterPerRow();

    /// <summary>
    /// The shipped shape: eligibility resolved once and passed to the overload,
    /// as the three view projections and the atomic-write guard loop now do.
    /// </summary>
    [Benchmark]
    public int FilterEligibility_Optimized_Hoisted() => FilterHoisted();

    /// <summary>
    /// The work the hoist removes, measured on its own. The end-to-end lanes
    /// above are dominated by the document scan the fast path performs once per
    /// referenced member, so the eligibility walk is only a few percent of them
    /// and cannot be resolved against machine noise. This pair charges the
    /// tree walk per row, exactly as the parameterless <c>Matches</c> does,
    /// against resolving it once - so the saving is attributable rather than
    /// inferred.
    /// </summary>
    [Benchmark]
    public int EligibilityWalk_Baseline_PerRow()
    {
        var eligible = 0;
        for (var i = 0; i < MutationRowCount; i++)
        {
            if (LatticePredicateEvaluator.IsFastPathEligible(_filter)) eligible++;
        }

        return eligible;
    }

    /// <summary>
    /// The shipped shape of the same work: the walk resolved once at
    /// construction and the answer reused for every row.
    /// </summary>
    [Benchmark]
    public int EligibilityWalk_Optimized_Hoisted()
    {
        var hoisted = LatticePredicateEvaluator.IsFastPathEligible(_filter);
        var eligible = 0;
        for (var i = 0; i < MutationRowCount; i++)
        {
            if (hoisted) eligible++;
        }

        return eligible;
    }

    // ========================================================================
    // (2) OR-map delta key surrogate per dot
    // ========================================================================

    /// <summary>
    /// The prior emitter body: the key surrogate re-encoded, into a fresh
    /// array, for every dot in the delta.
    /// </summary>
    [Benchmark]
    public int DeltaKeyBytes_Baseline_PerDot() => DecodeDeltaPerDot(_deltaWrapper).Count;

    /// <summary>
    /// The shipped shape through the <b>real production</b> decoder: a one-slot
    /// memo collapses each key's dot group to one encode and one array.
    /// </summary>
    [Benchmark]
    public int DeltaKeyBytes_Optimized_KeyMemo()
        => OrMapProvenanceDecoder.Instance.DecodeDeltas(_deltaWrapper).Count;

    // ========================================================================
    // (3) OR-map live-key projection tombstone test
    // ========================================================================

    /// <summary>
    /// The prior body on a <b>churned</b> map: every live dot tested by linear
    /// scan over the key's whole tombstone list.
    /// </summary>
    [Benchmark]
    public int CurrentValueChurned_Baseline_LinearScan()
        => ProjectLinearScan(_churnedMap).Count;

    /// <summary>
    /// The shipped shape through the <b>real production</b> decoder: above the
    /// threshold the key's tombstones are loaded once into a reused set.
    /// </summary>
    [Benchmark]
    public int CurrentValueChurned_Optimized_Indexed()
        => OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_churnedMap).Count;

    /// <summary>
    /// The control: a map whose keys sit below the threshold, where the shipped
    /// path keeps the linear scan and the two lanes must agree. A lane that
    /// "wins" here is measuring noise; a lane that loses means the threshold
    /// taxes the common case.
    /// </summary>
    [Benchmark]
    public int CurrentValueQuiet_Baseline_LinearScan()
        => ProjectLinearScan(_quietMap).Count;

    /// <summary>The shipped path on the un-churned control.</summary>
    [Benchmark]
    public int CurrentValueQuiet_Optimized_Indexed()
        => OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_quietMap).Count;

    // ========================================================================
    // lane bodies
    // ========================================================================

    private int FilterPerRow()
    {
        var kept = 0;
        for (var i = 0; i < _rows.Length; i++)
        {
            if (LatticePredicateEvaluator.Matches(_rows[i], _filter)) kept++;
        }

        return kept;
    }

    private int FilterHoisted()
    {
        var kept = 0;
        for (var i = 0; i < _rows.Length; i++)
        {
            if (LatticePredicateEvaluator.Matches(_rows[i], _filter, _filterEligible)) kept++;
        }

        return kept;
    }

    // The prior EmitDeltaTyped body, reproduced behind the same dispatch
    // production pays - a null check plus a per-type emitter lookup in a
    // ConcurrentDictionary and a delegate invoke over the boxed delta - so the
    // pair isolates the key-surrogate memo and nothing else. The private
    // generic emitter cannot be composed from outside the decoder, so the
    // reproduction stands in for it; the equivalence assertion in Setup pins it
    // to the shipped output element for element.
    private static readonly ConcurrentDictionary<Type, Action<object, HybridLogicalClock?, List<CrdtMemberChange>>> BaselineDeltaEmitters = new();

    private static IReadOnlyList<CrdtMemberChange> DecodeDeltaPerDot(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>();
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var boxed = entry.Delta;
            var emitter = BaselineDeltaEmitters.GetOrAdd(boxed.GetType(), static _ => EmitDeltaPerDot);
            emitter(boxed, entry.WallClock, result);
        }

        return result.Count == 0 ? Array.Empty<CrdtMemberChange>() : result;
    }

    private static void EmitDeltaPerDot(object boxed, HybridLogicalClock? wallClock, List<CrdtMemberChange> sink)
    {
        var delta = (OrMapDelta<string, GCounter>)boxed;
        var adds = delta.Adds;
        var tombstones = delta.Tombstones;
        sink.EnsureCapacity(sink.Count + adds.Count + tombstones.Count);

        for (var i = 0; i < adds.Count; i++)
        {
            var add = adds[i];
            sink.Add(new CrdtMemberChange
            {
                Element = KeyBytes(add.Key),
                Kind = CrdtMemberChangeKind.Added,
                ReplicaId = add.ReplicaId,
                Ordinal = add.Counter,
                WallClock = wallClock,
            });
        }

        for (var i = 0; i < tombstones.Count; i++)
        {
            var tomb = tombstones[i];
            sink.Add(new CrdtMemberChange
            {
                Element = KeyBytes(tomb.Key),
                Kind = CrdtMemberChangeKind.Removed,
                ReplicaId = tomb.ReplicaId,
                Ordinal = tomb.Counter,
                WallClock = wallClock,
            });
        }
    }

    // The prior EmitCurrentValueTyped body, reproduced behind the same
    // dispatch production pays - a null check, a per-type emitter lookup in a
    // ConcurrentDictionary, a delegate invoke over the boxed map, and the
    // element-bytes sort - so the pair isolates the tombstone test and nothing
    // else. The private generic emitter cannot be composed from outside the
    // decoder, so the reproduction stands in for it; the equivalence assertion
    // in Setup pins it to the shipped output member for member.
    private static readonly ConcurrentDictionary<Type, Action<object, List<CrdtMemberValue>>> BaselineEmitters = new();

    private static IReadOnlyList<CrdtMemberValue> ProjectLinearScan(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var emitter = BaselineEmitters.GetOrAdd(state.GetType(), static _ => EmitLinearScan);
        var result = new List<CrdtMemberValue>();
        emitter(state, result);
        if (result.Count == 0) return Array.Empty<CrdtMemberValue>();
        result.Sort(static (x, y) => CompareBytes(x.Element, y.Element));
        return result;
    }

    private static void EmitLinearScan(object boxed, List<CrdtMemberValue> sink)
    {
        var map = (OrMap<string, GCounter>)boxed;
        sink.EnsureCapacity(sink.Count + map.Adds.Count);

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            map.Tombstones.TryGetValue(key, out var tomb);

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                if (IsTombstonedLinear(tomb, entry.ReplicaId, entry.Counter)) continue;
                if (!hasLive
                    || entry.Counter > bestCounter
                    || (entry.Counter == bestCounter && string.CompareOrdinal(entry.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = entry.ReplicaId;
                    bestCounter = entry.Counter;
                }
            }

            if (!hasLive) continue;
            sink.Add(new CrdtMemberValue
            {
                Element = KeyBytes(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }
    }

    private static bool IsTombstonedLinear(List<OrSetDot>? tombstones, string replicaId, long counter)
    {
        if (tombstones is null) return false;
        for (var i = 0; i < tombstones.Count; i++)
        {
            var t = tombstones[i];
            if (t.Counter == counter && string.Equals(t.ReplicaId, replicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    // ========================================================================
    // shared helpers
    // ========================================================================

    private static byte[] KeyBytes(string key)
        => key.Length == 0 ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(key);

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

    private static OrMapDelta<string, GCounter> MakeGroupedDelta()
    {
        var adds = new List<OrMapDeltaEntry<string, GCounter>>(DeltaKeyCount * DotsPerKey);
        var tombs = new List<OrMapDeltaTombstone<string>>(DeltaKeyCount * DotsPerKey);
        for (var k = 0; k < DeltaKeyCount; k++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"entity/{k:D6}/member");
            for (var d = 0; d < DotsPerKey; d++)
            {
                adds.Add(new OrMapDeltaEntry<string, GCounter>
                {
                    Key = key,
                    ReplicaId = ReplicaA,
                    Counter = (k * DotsPerKey) + d + 1,
                    Value = new GCounter(),
                });
                tombs.Add(new OrMapDeltaTombstone<string>
                {
                    Key = key,
                    ReplicaId = ReplicaA,
                    Counter = (k * DotsPerKey) + d,
                });
            }
        }

        return new OrMapDelta<string, GCounter> { Adds = adds, Tombstones = tombs };
    }

    // Adjacent dots never share a key, so the memo misses on every dot. It is
    // the memo's worst case for time and its only correctness risk.
    private static OrMapDelta<string, GCounter> MakeInterleavedDelta()
    {
        var adds = new List<OrMapDeltaEntry<string, GCounter>>(DeltaKeyCount * DotsPerKey);
        var tombs = new List<OrMapDeltaTombstone<string>>(DeltaKeyCount * DotsPerKey);
        for (var d = 0; d < DotsPerKey; d++)
        {
            for (var k = 0; k < DeltaKeyCount; k++)
            {
                var key = string.Create(CultureInfo.InvariantCulture, $"entity/{k:D6}/member");
                adds.Add(new OrMapDeltaEntry<string, GCounter>
                {
                    Key = key,
                    ReplicaId = ReplicaA,
                    Counter = (k * DotsPerKey) + d + 1,
                    Value = new GCounter(),
                });
                tombs.Add(new OrMapDeltaTombstone<string>
                {
                    Key = key,
                    ReplicaId = ReplicaA,
                    Counter = (k * DotsPerKey) + d,
                });
            }
        }

        return new OrMapDelta<string, GCounter> { Adds = adds, Tombstones = tombs };
    }

    private static OrMap<string, GCounter> MakeMap(int addsPerKey, int tombstonesPerKey)
    {
        var map = new OrMap<string, GCounter>();
        for (var k = 0; k < ProjectionKeyCount; k++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"entity/{k:D6}/member");
            var entries = new List<OrMapEntry<GCounter>>(addsPerKey);
            for (var i = 0; i < addsPerKey; i++)
            {
                entries.Add(new OrMapEntry<GCounter>
                {
                    ReplicaId = ReplicaA,
                    Counter = i + 1,
                    Value = new GCounter(),
                });
            }

            map.Adds[key] = entries;

            // The tombstoned dots are the oldest ones, so the surviving dot is
            // always found last - the scan's worst case, and the shape churn
            // actually produces.
            var dots = new List<OrSetDot>(tombstonesPerKey);
            for (var i = 0; i < tombstonesPerKey; i++)
            {
                dots.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
            }

            map.Tombstones[key] = dots;
        }

        return map;
    }

    // Tombstones spanning two replicas, so the shared-replica precondition
    // fails and the index must not be taken.
    private static OrMap<string, GCounter> MakeMultiReplicaMap()
    {
        var map = new OrMap<string, GCounter>();
        for (var k = 0; k < 8; k++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"entity/{k:D6}/member");
            var entries = new List<OrMapEntry<GCounter>>(ChurnedAddsPerKey);
            var dots = new List<OrSetDot>(ChurnedTombstonesPerKey);
            for (var i = 0; i < ChurnedAddsPerKey; i++)
            {
                var replica = (i % 2) == 0 ? ReplicaA : ReplicaB;
                entries.Add(new OrMapEntry<GCounter> { ReplicaId = replica, Counter = i + 1, Value = new GCounter() });
            }

            for (var i = 0; i < ChurnedTombstonesPerKey; i++)
            {
                dots.Add(new OrSetDot { ReplicaId = (i % 2) == 0 ? ReplicaA : ReplicaB, Counter = i + 1 });
            }

            map.Adds[key] = entries;
            map.Tombstones[key] = dots;
        }

        return map;
    }

    // Every tombstone carries one replica - so the index IS taken - while the
    // live dots include one on a different replica whose counter collides with
    // a tombstoned counter. A counter-only test without the replica guard would
    // wrongly drop it.
    private static OrMap<string, GCounter> MakeCounterCollisionMap()
    {
        var map = new OrMap<string, GCounter>();
        for (var k = 0; k < 8; k++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"entity/{k:D6}/member");
            var entries = new List<OrMapEntry<GCounter>>();
            var dots = new List<OrSetDot>();
            for (var i = 0; i < ChurnedTombstonesPerKey; i++)
            {
                entries.Add(new OrMapEntry<GCounter> { ReplicaId = ReplicaA, Counter = i + 1, Value = new GCounter() });
                dots.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i + 1 });
            }

            // Same counter as a tombstoned dot, different replica: still live.
            entries.Add(new OrMapEntry<GCounter> { ReplicaId = ReplicaB, Counter = 1, Value = new GCounter() });

            map.Adds[key] = entries;
            map.Tombstones[key] = dots;
        }

        return map;
    }

    private static void AssertSame(int baseline, int shipped, string lane)
    {
        if (baseline != shipped)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline answered {baseline}, shipped answered {shipped}.");
        }
    }

    private void AssertDeltaEquivalent() => AssertDeltaEquivalent(_delta, "grouped delta");

    private void AssertInterleavedDeltaEquivalent()
        => AssertDeltaEquivalent(MakeInterleavedDelta(), "interleaved delta");

    private static void AssertDeltaEquivalent(OrMapDelta<string, GCounter> delta, string lane)
    {
        var wrapper = new[] { new CrdtProvenanceDelta(delta, HybridLogicalClock.Zero) };
        var baseline = DecodeDeltaPerDot(wrapper);
        var shipped = OrMapProvenanceDecoder.Instance.DecodeDeltas(wrapper);

        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline emitted {baseline.Count} events, shipped emitted {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (CompareBytes(baseline[i].Element, shipped[i].Element) != 0
                || baseline[i].Kind != shipped[i].Kind
                || !string.Equals(baseline[i].ReplicaId, shipped[i].ReplicaId, StringComparison.Ordinal)
                || baseline[i].Ordinal != shipped[i].Ordinal)
            {
                throw new InvalidOperationException($"[{lane}] event {i} differs between baseline and shipped.");
            }
        }
    }

    private static void AssertProjectionEquivalent(OrMap<string, GCounter> map, string lane)
    {
        var baseline = ProjectLinearScan(map);
        var shipped = OrMapProvenanceDecoder.Instance.DecodeCurrentValue(map);

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
}
