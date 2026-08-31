using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state allocation trims made to warm dictionary /
/// set maintenance paths in <c>Orleans.Lattice</c> so their per-operation byte
/// deltas are measurable in the clear. The full end-to-end cluster benchmarks
/// (<see cref="LatticeMicroBenchmarks"/>) route each operation through Orleans
/// serialization, persistence, and task machinery and therefore allocate on the
/// order of tens of kilobytes per op, so a sub-kilobyte trim sits well below
/// their run-to-run noise floor and cannot be attributed there. Each benchmark
/// below reproduces exactly one optimized code shape against its prior shape
/// with no cluster in the loop, so the <c>Allocated</c> column is deterministic
/// and the baseline-vs-optimized delta is precisely the heap the production
/// change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>TagIndexReconcileGrain.PruneStaleBaselines</c> - the prior form issued
/// a <c>.Keys.All(keep.Contains)</c> method-group delegate on the no-drift path
/// and a <c>.Keys.Where(closure).ToList()</c> (closure + Where iterator) on the
/// drift path; a single manual pass over the dictionary removes both;
/// (2) <c>AggregationApplier.LargestSourceKey</c> - iterating the freshly decoded
/// inverse-shard map directly instead of through <c>.Keys</c> removes one
/// throwaway <c>KeyCollection</c> per approximate-mode eviction;
/// (3) <c>LatticeTagIndexContext.NormalizeTags</c> - a threshold-gated ordinal
/// linear-scan dedup removes the <c>HashSet</c> (and its bucket and entry arrays)
/// on the common small-tag write/query path.
/// </para>
/// <para>
/// Each benchmark mirrors the production code shape rather than calling it: the
/// three production methods are <see langword="private"/>, so the pairs
/// reproduce their exact loops. The stale-baseline pair measures the divergent
/// stale-set computation only; the subsequent <c>Remove</c> loop is identical
/// between the two shapes and allocates nothing, so excluding it changes neither
/// lane's <c>Allocated</c> figure.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=alloctrims</c> (or
/// <c>--suite alloctrims</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class DictionaryAllocationTrimBenchmarks
{
    // ---- (1) prune stale baselines: long-lived state dictionary ----
    // A long-lived fixture dictionary (as the grain's persistent Baselines map
    // is), so its cached .Keys collection is not itself charged per op and the
    // measured delta is purely the LINQ machinery each shape adds.
    private Dictionary<string, byte[]> _baselines = null!;
    private IReadOnlyList<string> _coveredClean = null!;   // every baseline still covered (no drift)
    private IReadOnlyList<string> _coveredDrift = null!;    // last quarter retired (drift)

    // ---- (2) largest source key: a fresh per-mutation inverse-shard map ----
    private Dictionary<int, string[]> _memberKeys = null!;

    // ---- (3) normalize tags: a realistic small tag set with duplicates ----
    private string[] _tags = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        const int baselineCount = 32;
        _baselines = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var clean = new List<string>(baselineCount);
        var drift = new List<string>(baselineCount);
        var keepCount = baselineCount - baselineCount / 4;
        for (var i = 0; i < baselineCount; i++)
        {
            var id = "tree-" + i.ToString("D4", CultureInfo.InvariantCulture);
            _baselines[id] = new byte[] { (byte)i };
            clean.Add(id);
            if (i < keepCount)
            {
                drift.Add(id);
            }
        }

        _coveredClean = clean;
        _coveredDrift = drift;

        _memberKeys = new Dictionary<int, string[]>();
        foreach (var n in new[] { 4, 16, 64 })
        {
            var keys = new string[n];
            for (var i = 0; i < n; i++)
            {
                keys[i] = "src-" + i.ToString("D4", CultureInfo.InvariantCulture);
            }

            _memberKeys[n] = keys;
        }

        // A realistic tag write/query: a handful of tags carrying duplicates so
        // the dedup pass has real work. Well within the small-set common case.
        _tags = new[] { "red", "green", "blue", "red", "hot", "cold", "warm", "red" };
    }

    // ------------------------------------------------------------------
    // (1) Prune stale baselines
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline, no-drift path: <c>keep.Count == count &amp;&amp; Keys.All(keep.Contains)</c>
    /// early-returns, allocating the <c>All</c> method-group delegate every
    /// reconcile begin even though nothing is stale.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Prune baselines clean: LINQ .All (baseline)")]
    public int PruneBaselinesClean_Linq()
    {
        var keep = new HashSet<string>(_coveredClean, StringComparer.Ordinal);
        if (keep.Count == _baselines.Count && _baselines.Keys.All(keep.Contains))
        {
            return 0;
        }

        return _baselines.Keys.Where(t => !keep.Contains(t)).ToList().Count;
    }

    /// <summary>
    /// Optimized, no-drift path: a single manual pass leaves the deferred stale
    /// list unallocated, so nothing beyond the membership set is allocated.
    /// </summary>
    [Benchmark(Description = "Prune baselines clean: manual pass (optimized)")]
    public int PruneBaselinesClean_Manual()
    {
        var keep = new HashSet<string>(_coveredClean, StringComparer.Ordinal);
        List<string>? stale = null;
        foreach (var (treeId, _) in _baselines)
        {
            if (!keep.Contains(treeId))
            {
                (stale ??= new List<string>()).Add(treeId);
            }
        }

        return stale?.Count ?? 0;
    }

    /// <summary>
    /// Baseline, drift path: the count guard short-circuits and
    /// <c>Keys.Where(closure).ToList()</c> allocates a capturing closure plus a
    /// Where iterator on top of the stale list.
    /// </summary>
    [Benchmark(Description = "Prune baselines drift: LINQ .Where.ToList (baseline)")]
    public int PruneBaselinesDrift_Linq()
    {
        var keep = new HashSet<string>(_coveredDrift, StringComparer.Ordinal);
        if (keep.Count == _baselines.Count && _baselines.Keys.All(keep.Contains))
        {
            return 0;
        }

        return _baselines.Keys.Where(t => !keep.Contains(t)).ToList().Count;
    }

    /// <summary>
    /// Optimized, drift path: the single manual pass builds only the stale list
    /// (identical to the baseline's) and none of the LINQ machinery around it.
    /// </summary>
    [Benchmark(Description = "Prune baselines drift: manual pass (optimized)")]
    public int PruneBaselinesDrift_Manual()
    {
        var keep = new HashSet<string>(_coveredDrift, StringComparer.Ordinal);
        List<string>? stale = null;
        foreach (var (treeId, _) in _baselines)
        {
            if (!keep.Contains(treeId))
            {
                (stale ??= new List<string>()).Add(treeId);
            }
        }

        return stale?.Count ?? 0;
    }

    // ------------------------------------------------------------------
    // (2) Largest source key over a fresh inverse-shard map
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var sourceKey in map.Keys)</c> over a fresh
    /// per-mutation map allocates a throwaway <c>KeyCollection</c> on first
    /// access.
    /// </summary>
    [Benchmark(Description = "Largest source key: .Keys (baseline)")]
    [Arguments(4)]
    [Arguments(16)]
    [Arguments(64)]
    public string LargestSourceKey_Keys(int n)
    {
        var map = BuildMap(n);
        var largest = string.Empty;
        var first = true;
        foreach (var sourceKey in map.Keys)
        {
            if (first || string.CompareOrdinal(sourceKey, largest) > 0)
            {
                largest = sourceKey;
                first = false;
            }
        }

        return largest;
    }

    /// <summary>
    /// Optimized: iterating the map's entries directly uses only the struct
    /// enumerator, allocating no <c>KeyCollection</c>.
    /// </summary>
    [Benchmark(Description = "Largest source key: direct (optimized)")]
    [Arguments(4)]
    [Arguments(16)]
    [Arguments(64)]
    public string LargestSourceKey_Direct(int n)
    {
        var map = BuildMap(n);
        var largest = string.Empty;
        var first = true;
        foreach (var (sourceKey, _) in map)
        {
            if (first || string.CompareOrdinal(sourceKey, largest) > 0)
            {
                largest = sourceKey;
                first = false;
            }
        }

        return largest;
    }

    private Dictionary<string, int> BuildMap(int n)
    {
        var keys = _memberKeys[n];
        var map = new Dictionary<string, int>(keys.Length, StringComparer.Ordinal);
        for (var i = 0; i < keys.Length; i++)
        {
            map[keys[i]] = i;
        }

        return map;
    }

    // ------------------------------------------------------------------
    // (3) Normalize a small tag set
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: a <c>HashSet</c> dedup allocates the set plus its bucket and
    /// entry arrays on every normalize, even for the tiny tag sets that dominate
    /// the tag write and query paths.
    /// </summary>
    [Benchmark(Description = "Normalize tags: HashSet dedup (baseline)")]
    public string[] NormalizeTags_HashSet()
    {
        var tags = _tags;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var list = new List<string>(tags.Length);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            if (seen.Add(tag))
            {
                list.Add(tag);
            }
        }

        return list.ToArray();
    }

    /// <summary>
    /// Optimized: an ordinal linear scan against the accumulating result dedups
    /// with no <c>HashSet</c>, preserving first-seen order and per-element
    /// validation exactly.
    /// </summary>
    [Benchmark(Description = "Normalize tags: linear scan (optimized)")]
    public string[] NormalizeTags_LinearScan()
    {
        var tags = _tags;
        var list = new List<string>(tags.Length);
        foreach (var tag in tags)
        {
            ValidateTag(tag);
            if (!ContainsOrdinal(list, tag))
            {
                list.Add(tag);
            }
        }

        return list.ToArray();
    }

    private static bool ContainsOrdinal(List<string> list, string value)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (string.Equals(list[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    // Mirrors LatticeTagIndexContext.ValidateTag (NUL-separator guard) so both
    // lanes pay the same validation cost and the delta is the dedup structure.
    private static void ValidateTag(string tag)
    {
        ArgumentException.ThrowIfNullOrEmpty(tag);
        if (tag.Contains('\0'))
        {
            throw new ArgumentException("A tag must not contain the NUL separator character.", nameof(tag));
        }
    }
}
