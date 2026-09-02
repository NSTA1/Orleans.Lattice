using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state allocation trims made to the grain-index
/// query executor and the state-API metrics observer so their per-operation byte
/// deltas are measurable in the clear. As with the sibling
/// <see cref="ViewMaintainerAllocationBenchmarks"/> suite, the full end-to-end
/// cluster benchmarks route each operation through Orleans serialization,
/// persistence, and task machinery and allocate on the order of tens of kilobytes
/// per op, so a sub-kilobyte trim sits below their run-to-run noise floor. Each
/// benchmark below reproduces exactly one optimized code shape against its prior
/// shape with no cluster in the loop, so the <c>Allocated</c> column is
/// deterministic and the baseline-vs-optimized delta is precisely the heap the
/// production change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>GrainIndexQueryExecutor.IntersectAsync</c> - the prior form grew the
/// per-clause <c>survivors</c> dictionary from empty as it folded each later
/// clause's scan, reallocating its bucket and entry arrays; because every
/// survivor is a key already present in the driving <c>candidates</c> set, its
/// current count is a tight upper bound, so presizing to it removes the regrowth
/// churn on each intersect pass of a multi-clause AND query;
/// (2) <c>LatticeStateMetricsObserver.ObserveAsync</c> - the prior form grew the
/// <c>changed</c> list from empty on every delta tick; presizing it to the
/// current sample's tree count (an upper bound on the changed set) removes that
/// list's regrowth;
/// (3) <c>LatticeStateMetricsObserver.ObserveAsync</c> - the prior form built the
/// removed-tree ids via <c>.Where(...).ToList().OrderBy(...).ToArray()</c>, whose
/// intermediate <c>.ToList()</c> is a throwaway list because <c>OrderBy</c>
/// already materialises and sorts its source; sorting the filtered sequence
/// directly into the result array removes that list.
/// </para>
/// <para>
/// The shapes are reproduced against stand-in record types (the production
/// value types are Orleans-serializable records whose own size is identical
/// between each pair's lanes, so the reported delta is exactly the collection
/// overhead the edit removes, not the payload). Run it via
/// <c>BENCH_MICROBENCH_SUITE=queryproj</c> (or <c>--suite queryproj</c>); see
/// <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast to
/// run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class QueryProjectionAllocationBenchmarks
{
    /// <summary>A minimal stand-in for the internal <c>GrainIndexMatch</c> record.</summary>
    private sealed record Match(string GrainKey);

    /// <summary>A minimal stand-in for the <c>TreeMetrics</c> record.</summary>
    private sealed record Metrics(string TreeId, long Value);

    // ---- (1) a populated driving set + a later clause's scan keys ----
    private Dictionary<string, Match> _candidates = null!;
    private string[] _secondClauseKeys = null!;

    // ---- (2)/(3) a current sample and its predecessor ----
    private Dictionary<string, Metrics> _current = null!;
    private Dictionary<string, Metrics> _previous = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A multi-clause AND query whose most selective clause buffers a few
        // hundred candidate grain keys; a later clause's scan then re-hits most
        // of them, so the survivor set climbs back toward the driving count.
        const int candidateCount = 512;
        _candidates = new Dictionary<string, Match>(candidateCount, StringComparer.Ordinal);
        for (var i = 0; i < candidateCount; i++)
        {
            var key = "grain-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _candidates[key] = new Match(key);
        }

        // The later clause scans every candidate key (all survive) plus a run of
        // misses, exactly as an overlapping range scan does.
        _secondClauseKeys = _candidates.Keys
            .Concat(Enumerable.Range(0, 128).Select(i => "miss-" + i.ToString("D5", CultureInfo.InvariantCulture)))
            .ToArray();

        // A metrics tick over a moderate tree roster where every tree's counters
        // moved (worst case for the changed list) and a handful of trees were
        // removed since the prior sample.
        const int treeCount = 256;
        _current = new Dictionary<string, Metrics>(treeCount, StringComparer.Ordinal);
        _previous = new Dictionary<string, Metrics>(treeCount + 16, StringComparer.Ordinal);
        for (var i = 0; i < treeCount; i++)
        {
            var treeId = "tree-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _current[treeId] = new Metrics(treeId, i + 1);
            _previous[treeId] = new Metrics(treeId, i); // different value => changed
        }

        for (var i = 0; i < 16; i++)
        {
            var goneId = "tree-gone-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _previous[goneId] = new Metrics(goneId, i);
        }
    }

    // ------------------------------------------------------------------
    // (1) Multi-clause AND intersect survivor set
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the survivor dictionary grows from empty as the later clause's
    /// scan folds in, reallocating its bucket and entry arrays.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Intersect: survivors grow from empty (baseline)")]
    public int Intersect_GrowFromEmpty()
    {
        var survivors = new Dictionary<string, Match>(StringComparer.Ordinal);
        foreach (var key in _secondClauseKeys)
        {
            if (_candidates.TryGetValue(key, out var driving))
            {
                survivors[key] = driving;
            }
        }

        return survivors.Count;
    }

    /// <summary>
    /// Optimized: presizing the survivor dictionary to the driving set's count
    /// (a tight upper bound) removes the grow-from-empty rehash churn; the fold
    /// body is identical.
    /// </summary>
    [Benchmark(Description = "Intersect: survivors presized (optimized)")]
    public int Intersect_Presized()
    {
        var survivors = new Dictionary<string, Match>(_candidates.Count, StringComparer.Ordinal);
        foreach (var key in _secondClauseKeys)
        {
            if (_candidates.TryGetValue(key, out var driving))
            {
                survivors[key] = driving;
            }
        }

        return survivors.Count;
    }

    // ------------------------------------------------------------------
    // (2) Metrics delta - changed list
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the changed list grows from empty on every delta tick.
    /// </summary>
    [Benchmark(Description = "Changed list: grow from empty (baseline)")]
    public int ChangedList_GrowFromEmpty()
    {
        var changed = new List<Metrics>();
        foreach (var pair in _current)
        {
            if (!_previous.TryGetValue(pair.Key, out var prior) || prior.Value != pair.Value.Value)
            {
                changed.Add(pair.Value);
            }
        }

        return changed.Count;
    }

    /// <summary>
    /// Optimized: presizing the changed list to the current sample's tree count
    /// (an upper bound on the changed set) removes the regrowth; the filter body
    /// is identical.
    /// </summary>
    [Benchmark(Description = "Changed list: presized (optimized)")]
    public int ChangedList_Presized()
    {
        var changed = new List<Metrics>(_current.Count);
        foreach (var pair in _current)
        {
            if (!_previous.TryGetValue(pair.Key, out var prior) || prior.Value != pair.Value.Value)
            {
                changed.Add(pair.Value);
            }
        }

        return changed.Count;
    }

    // ------------------------------------------------------------------
    // (3) Metrics delta - removed-tree ids
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: an intermediate <c>.ToList()</c> materialises the filtered keys
    /// into a throwaway list before <c>OrderBy(...).ToArray()</c> re-materialises
    /// and sorts them.
    /// </summary>
    [Benchmark(Description = "Removed ids: .ToList().OrderBy().ToArray() (baseline)")]
    public int RemovedIds_ToListThenSort()
    {
        var removed = _previous.Keys.Where(id => !_current.ContainsKey(id)).ToList();
        var ordered = removed.OrderBy(static id => id, StringComparer.Ordinal).ToArray();
        return ordered.Length;
    }

    /// <summary>
    /// Optimized: <c>OrderBy</c> already materialises and sorts its source, so the
    /// filtered sequence is sorted directly into the result array with no
    /// intermediate list.
    /// </summary>
    [Benchmark(Description = "Removed ids: .OrderBy().ToArray() (optimized)")]
    public int RemovedIds_SortDirect()
    {
        var ordered = _previous.Keys
            .Where(id => !_current.ContainsKey(id))
            .OrderBy(static id => id, StringComparer.Ordinal)
            .ToArray();
        return ordered.Length;
    }
}
