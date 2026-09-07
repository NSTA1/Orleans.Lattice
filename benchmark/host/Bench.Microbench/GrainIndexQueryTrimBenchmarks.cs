using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.GrainIndex;
using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three allocation reductions made to the <b>grain-index query
/// path</b>: the AND-intersect pass the executor runs per scanned entry, the
/// per-property accumulators the planner builds per conjunction, and the
/// interval algebra every clause's key ranges are folded through.
/// <para>
/// The shape that makes the intersect worth trimming is that it is the only part
/// of the query path whose cost is <b>per scanned entry of a non-driving
/// clause</b>. A conjunction over two properties becomes two scans whose grain
/// keys are intersected, and the later scan is the wide one: it is by
/// construction less selective than the driving clause, so most of what it
/// yields is discarded. Every discarded entry still paid for a grain-key
/// substring and a <see cref="GrainIndexMatch"/> to carry it, both built purely
/// to probe a dictionary and then dropped.
/// </para>
/// <para>
/// Judge the suite on <b>Allocated</b>. These are allocation trims, the
/// allocated column reproduces bit-for-bit across rounds, and Mean on a shared
/// developer host does not. Nothing here starts a silo, so the suite is cheap
/// enough to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> The intersect lane's helpers are private members of
/// <c>GrainIndexQueryExecutor</c> reached only through an
/// <see cref="IAsyncEnumerable{T}"/> over a live tree, so its A/B arms reproduce
/// the per-entry bodies here and drive both from the <b>same</b> pre-built entry
/// keys - the strings a page of a key-only scan hands back. Both arms seed the
/// candidate set identically and differ only in the intersect pass itself; a
/// baseline arm that skipped part of the optimized arm's shell would fabricate a
/// regression. The planner and range-set lanes need no copy on the optimized
/// side: they call the <b>real shipped</b>
/// <see cref="GrainIndexQueryPlanner"/> and <see cref="GrainIndexRangeSet"/>
/// through <c>InternalsVisibleTo</c>, and the planner lane additionally ships a
/// <c>_Production</c> arm that plans a real lambda end to end, pinning the
/// copied accumulator shell to reality.
/// </para>
/// <para>
/// The three edits under test:
/// (1) an intersect pass materialised a grain-key substring and a
/// <see cref="GrainIndexMatch"/> for every entry the non-driving clause yielded,
/// then probed the candidate map twice per survivor (read, then write into a
/// freshly-allocated survivor map). It now probes through the dictionary's
/// <c>ReadOnlySpan&lt;char&gt;</c> alternate lookup over the tree's own key
/// string - no substring, no match - stamps the surviving slot in place through
/// <see cref="CollectionsMarshal.GetValueRefOrNullRef{TKey, TValue}"/> for one
/// probe per hit, and prunes the set in place instead of rebuilding it, so a
/// C-clause conjunction no longer allocates C-1 dictionaries and their rehashes;
/// (2) a conjunction accumulated its per-property state into three parallel
/// arrays sized to the projected property count - ranges, residuals, point-lookup
/// flags - and re-indexed all three up to five times per atom. They are now one
/// array of a slot struct, bound once per atom by reference;
/// (3) the interval algebra built every result through a
/// <c>List&lt;GrainIndexKeyRange&gt;</c> that was then copied out with
/// <c>ToArray</c>, three allocations for a result that is one range when a
/// conjunction narrows a property and two when a negation complements a point.
/// Both now accumulate the first two ranges inline and allocate only the
/// exact-width array they return.
/// </para>
/// <para>
/// <b>Contrast arm.</b> <see cref="Intersect_Contrast_SpanProbeRebuild"/> takes
/// only the cheap half of edit (1) - it probes by span, eliminating the
/// per-entry substring and match, but still rebuilds a survivor dictionary on
/// every pass - so the report can show whether the in-place prune earns its
/// complexity or whether the span probe alone would have done.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=grainindexquerytrims</c> (or
/// <c>--suite grainindexquerytrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class GrainIndexQueryTrimBenchmarks
{
    /// <summary>
    /// Grains surviving the driving (most selective) clause. This is the set the
    /// later clauses probe against, so it is the working set that stays hot.
    /// </summary>
    private const int DrivingWidth = 512;

    /// <summary>
    /// Entries a non-driving clause yields. It is the less selective clause by
    /// construction - that is why the planner scheduled it second - so it is
    /// several times wider than the candidate set, and most of what it yields is
    /// discarded.
    /// </summary>
    private const int ScanWidth = 8192;

    /// <summary>Non-driving clauses in the conjunction, i.e. an AND over three properties.</summary>
    private const int LaterClauses = 2;

    private const string DrivingProperty = "Country";

    private static readonly string[] LaterProperties = ["Age", "Status"];

    private GrainIndexMatch[] _driving = [];
    private string[][] _scans = [];

    private GrainIndexQueryProperty[] _properties = [];
    private string[] _propertyNames = [];
    private Expression<Func<BenchIndexState, bool>> _predicate = _ => true;
    private AnalysedAtom[] _atoms = [];
    private string[][] _memberPathAtoms = [];

    private GrainIndexKeyRange[] _intersectLeft = [];
    private GrainIndexKeyRange[] _intersectRight = [];
    private GrainIndexKeyRange[] _complementPoint = [];

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(20260218);

        // The driving clause's matches, keyed by grain key exactly as the
        // executor buffers them. Payloads are shared: the intersect never reads
        // one, and giving each arm its own would measure the fixture.
        byte[] payload = new byte[48];
        random.NextBytes(payload);

        _driving = new GrainIndexMatch[DrivingWidth];
        for (var i = 0; i < DrivingWidth; i++)
        {
            _driving[i] = new GrainIndexMatch(GrainKey(i), DrivingProperty, payload);
        }

        // Each later clause scans a wide range. Its first slice overlaps the
        // candidate set - so the pass has real survivors to stamp and real
        // casualties to prune - and the rest falls outside it, which is the
        // majority case the per-entry allocation was being paid on.
        _scans = new string[LaterClauses][];
        for (var clause = 0; clause < LaterClauses; clause++)
        {
            string property = LaterProperties[clause];
            var keys = new string[ScanWidth];

            // Each pass keeps three quarters of what the previous one left, so
            // both passes have real survivors to stamp and real casualties to
            // sweep - neither trivially skips the prune.
            int overlap = (DrivingWidth * 3) >> (2 + clause);
            for (var i = 0; i < ScanWidth; i++)
            {
                int grain = i < overlap ? i : DrivingWidth + i;
                keys[i] = GrainIndexKeyEncoder.ComposeKey(
                    property,
                    GrainIndexKeyEncoder.EncodeValue(grain % 97),
                    GrainKey(grain));
            }

            _scans[clause] = keys;
        }

        // The planner lane's fixture: a four-property index and a three-atom
        // conjunction over three of them, which is the shape that produces a
        // multi-clause intersect in the first place.
        _properties =
        [
            new GrainIndexQueryProperty(0, "Age", typeof(int)),
            new GrainIndexQueryProperty(1, "Country", typeof(string)),
            new GrainIndexQueryProperty(2, "Score", typeof(double)),
            new GrainIndexQueryProperty(3, "Status", typeof(int)),
        ];
        _propertyNames = ["Age", "Country", "Score", "Status"];
        _predicate = s => s.Age >= 18 && s.Country == "GB" && s.Status == 2;

        // The per-atom analyses a conjunction accumulates, captured once so the
        // A/B arms measure the accumulator and not the expression translation
        // that produced them.
        _atoms =
        [
            new AnalysedAtom(0, Ranges("Age", 18), true),
            new AnalysedAtom(1, Ranges("Country", "GB"), true),
            new AnalysedAtom(3, Ranges("Status", 2), true),
        ];

        // The member paths each analysed atom names, in the proportion a real
        // conjunction produces them: a comparison names one member on each side
        // of the operator, so the same path arrives twice and dedups to one.
        // The last atom is the rarer two-member case that still has to spill.
        _memberPathAtoms =
        [
            ["Age", "Age"],
            ["Country", "Country"],
            ["Status", "Status"],
            ["Score", "Score"],
            ["Age", "Score"],
        ];

        // The range-set lane's fixture: an intersection that narrows to one
        // range, and a complement of a point lookup that leaves the two gaps
        // either side of it. Both are the dominant shapes.
        _intersectLeft = Ranges("Age", 18);
        _intersectRight = [new GrainIndexKeyRange(
            GrainIndexKeyEncoder.PropertyRangeStartInclusive("Age"),
            GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", GrainIndexKeyEncoder.EncodeValue(65)))];
        _complementPoint =
        [
            new GrainIndexKeyRange(
                GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", GrainIndexKeyEncoder.EncodeValue(18)),
                GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", GrainIndexKeyEncoder.EncodeValue(18))),
        ];
    }

    // ---------------------------------------------------------------------
    // Lane 1: the AND-intersect pass, per scanned entry.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: every scanned entry materialises a grain-key
    /// substring and a match to carry it, and each survivor costs two probes into
    /// a survivor dictionary allocated afresh on every pass.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int Intersect_Baseline_SubstringRebuild()
    {
        var candidates = new Dictionary<string, GrainIndexMatch>(StringComparer.Ordinal);
        for (var i = 0; i < _driving.Length; i++)
        {
            candidates[_driving[i].GrainKey] = _driving[i];
        }

        for (var pass = 0; pass < _scans.Length && candidates.Count > 0; pass++)
        {
            var scan = _scans[pass];
            var survivors = new Dictionary<string, GrainIndexMatch>(candidates.Count, StringComparer.Ordinal);
            for (var i = 0; i < scan.Length; i++)
            {
                // The substring and the match were built inside the scan, before
                // the caller could know whether the entry was a candidate.
                if (!TryReadGrainKey(scan[i], out string grainKey))
                    continue;

                var scanned = new GrainIndexMatch(grainKey, "scan", []);
                if (candidates.TryGetValue(scanned.GrainKey, out var driving))
                {
                    survivors[scanned.GrainKey] = driving;
                }
            }

            candidates = survivors;
        }

        return candidates.Count;
    }

    /// <summary>
    /// The shipped shape: probe through a span over the tree's own key string, so
    /// nothing is allocated per scanned entry, stamp the surviving slot in place
    /// for one probe per hit, and prune the set rather than rebuild it.
    /// </summary>
    [Benchmark]
    public int Intersect_Optimized_SpanProbeInPlacePrune()
    {
        var candidates = new Dictionary<string, Candidate>(StringComparer.Ordinal);
        for (var i = 0; i < _driving.Length; i++)
        {
            candidates[_driving[i].GrainKey] = new Candidate(_driving[i]);
        }

        var lookup = candidates.GetAlternateLookup<ReadOnlySpan<char>>();

        for (var pass = 1; pass <= _scans.Length && candidates.Count > 0; pass++)
        {
            var scan = _scans[pass - 1];
            var survivors = 0;
            for (var i = 0; i < scan.Length; i++)
            {
                if (!TryReadGrainKey(scan[i], out ReadOnlySpan<char> grainKey))
                    continue;

                ref var candidate = ref CollectionsMarshal.GetValueRefOrNullRef(lookup, grainKey);
                if (Unsafe.IsNullRef(ref candidate) || candidate.LastPass != pass - 1)
                    continue;

                candidate.LastPass = pass;
                survivors++;
            }

            if (survivors == candidates.Count)
                continue;

            if (survivors == 0)
            {
                candidates.Clear();
                break;
            }

            foreach (var pair in candidates)
            {
                if (pair.Value.LastPass != pass)
                {
                    candidates.Remove(pair.Key);
                }
            }
        }

        return candidates.Count;
    }

    /// <summary>
    /// The rejected cheaper alternative: take the span probe, which is what
    /// removes the per-entry substring and match, but keep rebuilding a survivor
    /// dictionary on every pass. Shows what the in-place prune adds on its own.
    /// </summary>
    [Benchmark]
    public int Intersect_Contrast_SpanProbeRebuild()
    {
        var candidates = new Dictionary<string, Candidate>(StringComparer.Ordinal);
        for (var i = 0; i < _driving.Length; i++)
        {
            candidates[_driving[i].GrainKey] = new Candidate(_driving[i]);
        }

        for (var pass = 0; pass < _scans.Length && candidates.Count > 0; pass++)
        {
            var scan = _scans[pass];
            var lookup = candidates.GetAlternateLookup<ReadOnlySpan<char>>();
            var survivors = new Dictionary<string, Candidate>(candidates.Count, StringComparer.Ordinal);
            for (var i = 0; i < scan.Length; i++)
            {
                if (!TryReadGrainKey(scan[i], out ReadOnlySpan<char> grainKey))
                    continue;

                ref var candidate = ref CollectionsMarshal.GetValueRefOrNullRef(lookup, grainKey);
                if (Unsafe.IsNullRef(ref candidate))
                    continue;

                survivors[candidate.Match.GrainKey] = candidate;
            }

            candidates = survivors;
        }

        return candidates.Count;
    }

    // ---------------------------------------------------------------------
    // Lane 2: the planner's per-property conjunction accumulators.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: three parallel arrays sized to the projected
    /// property count, re-indexed up to five times per atom.
    /// </summary>
    [Benchmark]
    public int Conjunction_Baseline_ParallelArrays()
    {
        var ranges = new GrainIndexKeyRange[_properties.Length][];
        var residuals = new object?[_properties.Length];
        var pointLookups = new bool[_properties.Length];
        var touched = 0;

        for (var i = 0; i < _atoms.Length; i++)
        {
            var atom = _atoms[i];
            int ordinal = atom.Ordinal;

            if (ranges[ordinal] is null)
            {
                touched++;
                ranges[ordinal] = atom.Ranges;
            }
            else
            {
                ranges[ordinal] = GrainIndexRangeSet.Intersect(ranges[ordinal]!, atom.Ranges);
            }

            if (ranges[ordinal]!.Length == 0)
                return 0;

            residuals[ordinal] = null;
            pointLookups[ordinal] |= atom.PointLookup;
        }

        var kept = 0;
        for (var ordinal = 0; ordinal < _properties.Length; ordinal++)
        {
            if (ranges[ordinal] is not null && pointLookups[ordinal])
            {
                kept++;
            }
        }

        return touched + kept;
    }

    /// <summary>
    /// The shipped shape: one slot array, bound once per atom by reference.
    /// </summary>
    [Benchmark]
    public int Conjunction_Optimized_SlotArray()
    {
        var slots = new ConjunctionSlot[_properties.Length];
        var touched = 0;

        for (var i = 0; i < _atoms.Length; i++)
        {
            var atom = _atoms[i];
            ref var slot = ref slots[atom.Ordinal];

            if (slot.Ranges is null)
            {
                touched++;
                slot.Ranges = atom.Ranges;
            }
            else
            {
                slot.Ranges = GrainIndexRangeSet.Intersect(slot.Ranges, atom.Ranges);
            }

            if (slot.Ranges.Length == 0)
                return 0;

            slot.Residual = null;
            slot.PointLookup |= atom.PointLookup;
        }

        var kept = 0;
        for (var ordinal = 0; ordinal < _properties.Length; ordinal++)
        {
            ref var slot = ref slots[ordinal];
            if (slot.Ranges is not null && slot.PointLookup)
            {
                kept++;
            }
        }

        return touched + kept;
    }

    /// <summary>
    /// The real shipped planner, end to end over a real lambda. Its Allocated
    /// should track the optimized arm's direction, which is what pins the copied
    /// accumulator shell above to production.
    /// </summary>
    [Benchmark]
    public int Conjunction_Production_RealPlanner()
    {
        var plan = GrainIndexQueryPlanner.Build(_predicate, "Bench", _properties, _propertyNames);
        return plan.Disjuncts.Length;
    }

    // ---------------------------------------------------------------------
    // Lane 4: the planner's per-atom member-path collection.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: a fresh list per analysed atom, even though
    /// the overwhelmingly dominant atom names exactly one member path.
    /// </summary>
    [Benchmark]
    public int MemberPaths_Baseline_ListPerAtom()
    {
        var total = 0;

        for (var i = 0; i < _memberPathAtoms.Length; i++)
        {
            var atom = _memberPathAtoms[i];
            var paths = new List<string>(1);

            for (var j = 0; j < atom.Length; j++)
            {
                var path = atom[j];
                var seen = false;

                for (var k = 0; k < paths.Count; k++)
                {
                    if (string.Equals(paths[k], path, StringComparison.OrdinalIgnoreCase))
                    {
                        seen = true;
                        break;
                    }
                }

                if (!seen)
                    paths.Add(path);
            }

            if (paths.Count == 0 || paths.Count > 1)
                continue;

            total += paths[0].Length;
        }

        return total;
    }

    /// <summary>
    /// The shipped shape: the first path is held inline in a struct and the set
    /// spills to a list only when an atom genuinely names two distinct members.
    /// </summary>
    [Benchmark]
    public int MemberPaths_Optimized_InlineSet()
    {
        var total = 0;

        for (var i = 0; i < _memberPathAtoms.Length; i++)
        {
            var atom = _memberPathAtoms[i];
            var paths = default(BenchMemberPathSet);

            for (var j = 0; j < atom.Length; j++)
            {
                paths.Add(atom[j]);
            }

            if (paths.IsEmpty || paths.HasMoreThanOne)
                continue;

            total += paths.Single.Length;
        }

        return total;
    }

    /// <summary>
    /// Mirrors the shipped <c>GrainIndexQueryPlanner.MemberPathSet</c>, which is
    /// private to the planner. Both arms above share this shell's call shape so
    /// the only difference measured is where the first path lives.
    /// </summary>
    private struct BenchMemberPathSet
    {
        private string? _first;
        private List<string>? _rest;

        internal readonly bool IsEmpty => _first is null;

        internal readonly bool HasMoreThanOne => _rest is not null;

        internal readonly string Single => _first!;

        internal void Add(string path)
        {
            if (_first is null)
            {
                _first = path;
                return;
            }

            if (string.Equals(_first, path, StringComparison.OrdinalIgnoreCase))
                return;

            if (_rest is null)
            {
                _rest = [path];
                return;
            }

            for (var i = 0; i < _rest.Count; i++)
            {
                if (string.Equals(_rest[i], path, StringComparison.OrdinalIgnoreCase))
                    return;
            }

            _rest.Add(path);
        }
    }

    // ---------------------------------------------------------------------
    // Lane 3: the interval algebra behind every clause's key ranges.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: grow a list, then copy it out.
    /// </summary>
    [Benchmark]
    public int RangeSet_Baseline_ListThenToArray()
    {
        var intersected = BaselineIntersect(_intersectLeft, _intersectRight);
        var complemented = BaselineComplement(
            _complementPoint,
            GrainIndexKeyEncoder.PropertyRangeStartInclusive("Age"),
            GrainIndexKeyEncoder.PropertyRangeEndExclusive("Age"));

        return intersected.Length + complemented.Length;
    }

    /// <summary>
    /// The real shipped algebra: accumulate the first two ranges inline and
    /// allocate only the exact-width array returned.
    /// </summary>
    [Benchmark]
    public int RangeSet_Optimized_InlineAccumulator()
    {
        var intersected = GrainIndexRangeSet.Intersect(_intersectLeft, _intersectRight);
        var complemented = GrainIndexRangeSet.Complement(
            _complementPoint,
            GrainIndexKeyEncoder.PropertyRangeStartInclusive("Age"),
            GrainIndexKeyEncoder.PropertyRangeEndExclusive("Age"));

        return intersected.Length + complemented.Length;
    }

    // ---------------------------------------------------------------------
    // Reproduced shells and fixtures.
    // ---------------------------------------------------------------------

    private static GrainIndexKeyRange[] BaselineIntersect(GrainIndexKeyRange[] left, GrainIndexKeyRange[] right)
    {
        if (left.Length == 0 || right.Length == 0)
            return [];

        List<GrainIndexKeyRange>? overlaps = null;
        var i = 0;
        var j = 0;
        while (i < left.Length && j < right.Length)
        {
            var a = left[i];
            var b = right[j];

            string start = string.CompareOrdinal(a.StartInclusive, b.StartInclusive) >= 0
                ? a.StartInclusive
                : b.StartInclusive;
            string end = string.CompareOrdinal(a.EndExclusive, b.EndExclusive) <= 0
                ? a.EndExclusive
                : b.EndExclusive;

            if (string.CompareOrdinal(start, end) < 0)
            {
                overlaps ??= new List<GrainIndexKeyRange>(2);
                overlaps.Add(new GrainIndexKeyRange(start, end));
            }

            if (string.CompareOrdinal(a.EndExclusive, b.EndExclusive) <= 0)
            {
                i++;
            }
            else
            {
                j++;
            }
        }

        return overlaps is null ? [] : overlaps.ToArray();
    }

    private static GrainIndexKeyRange[] BaselineComplement(
        GrainIndexKeyRange[] ranges,
        string universeStart,
        string universeEnd)
    {
        if (ranges.Length == 0)
            return [new GrainIndexKeyRange(universeStart, universeEnd)];

        List<GrainIndexKeyRange>? gaps = null;
        string cursor = universeStart;
        for (var i = 0; i < ranges.Length; i++)
        {
            var range = ranges[i];
            if (string.CompareOrdinal(cursor, range.StartInclusive) < 0)
            {
                gaps ??= new List<GrainIndexKeyRange>(ranges.Length + 1);
                gaps.Add(new GrainIndexKeyRange(cursor, range.StartInclusive));
            }

            if (string.CompareOrdinal(range.EndExclusive, cursor) > 0)
            {
                cursor = range.EndExclusive;
            }
        }

        if (string.CompareOrdinal(cursor, universeEnd) < 0)
        {
            gaps ??= new List<GrainIndexKeyRange>(1);
            gaps.Add(new GrainIndexKeyRange(cursor, universeEnd));
        }

        return gaps is null ? [] : gaps.ToArray();
    }

    private static bool TryReadGrainKey(string key, out string grainKey)
    {
        if (!TryReadGrainKey(key, out ReadOnlySpan<char> span))
        {
            grainKey = string.Empty;
            return false;
        }

        grainKey = new string(span);
        return true;
    }

    private static bool TryReadGrainKey(string key, out ReadOnlySpan<char> grainKey)
    {
        int first = key.IndexOf(GrainIndexKeyEncoder.Separator);
        if (first < 0)
        {
            grainKey = default;
            return false;
        }

        int second = key.IndexOf(GrainIndexKeyEncoder.Separator, first + 1);
        if (second < 0)
        {
            grainKey = default;
            return false;
        }

        grainKey = key.AsSpan(second + 1);
        return true;
    }

    private static GrainIndexKeyRange[] Ranges<TValue>(string property, TValue value)
    {
        string encoded = GrainIndexKeyEncoder.EncodeValue(value);
        return
        [
            new GrainIndexKeyRange(
                GrainIndexKeyEncoder.ValueRangeStartInclusive(property, encoded),
                GrainIndexKeyEncoder.PropertyRangeEndExclusive(property)),
        ];
    }

    private static string GrainKey(int ordinal) =>
        string.Create(
            null,
            stackalloc char[32],
            $"subject-{ordinal:D8}");

    private struct Candidate(GrainIndexMatch match)
    {
        internal GrainIndexMatch Match = match;
        internal int LastPass = 0;
    }

    private struct ConjunctionSlot
    {
        internal GrainIndexKeyRange[]? Ranges;
        internal object? Residual;
        internal bool PointLookup;
    }

    private readonly record struct AnalysedAtom(int Ordinal, GrainIndexKeyRange[] Ranges, bool PointLookup);

    /// <summary>The state shape the planner lane plans against.</summary>
    public sealed class BenchIndexState
    {
        /// <summary>An order-preserving integer property.</summary>
        public int Age { get; set; }

        /// <summary>A string property, which routes as an exact range.</summary>
        public string Country { get; set; } = string.Empty;

        /// <summary>A floating-point property, which keeps its residual predicate.</summary>
        public double Score { get; set; }

        /// <summary>A second integer property, so the conjunction spans three.</summary>
        public int Status { get; set; }
    }
}
