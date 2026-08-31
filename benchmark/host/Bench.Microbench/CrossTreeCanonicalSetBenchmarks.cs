using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the steady-state allocation trim made to the cross-tree and view
/// coordination barriers, whose per-call byte delta is invisible in the
/// end-to-end cluster benchmarks (each barrier registration routes through
/// Orleans serialization and persistence, allocating orders of magnitude more
/// than this trim, so the trim sits below their noise floor).
/// <para>
/// Three production sites - <c>ViewCrossTreeCoordinatorGrain</c> and
/// <c>LatticeCrossTreeReceiverGrain</c> freezing their wait set, and
/// <c>LatticeCrossTreeTxGrain</c> stamping its participant tree-id set - each
/// canonicalised a string set with
/// <c>source.Distinct(Ordinal).OrderBy(Ordinal).ToList()/.ToArray()</c>. That
/// LINQ form allocates, per call, an <c>OrderedEnumerable</c> wrapper, a
/// materialised element buffer, a projected key array, and an integer
/// sort-index map - all on top of the result collection. The optimized shape
/// (<see cref="CanonicalStringSet"/>, reached here through the benchmark's
/// <c>InternalsVisibleTo</c> grant, so this measures the real production code)
/// de-duplicates through a single <see cref="HashSet{T}"/> pass and sorts in
/// place, allocating only the result plus the transient dedup set. The output
/// is byte-for-byte identical.
/// </para>
/// <para>
/// The two <c>Baseline_*</c>/<c>Optimized_*</c> pairs cover the two result
/// shapes: the <c>List</c> pair mirrors the two wait-set freezes, and the
/// <c>Array</c> pair mirrors the participant set (which also projects
/// <c>Select(p =&gt; p.TreeId)</c> off a participant list). <see cref="SetSize"/>
/// sweeps realistic barrier widths; inputs carry duplicates so the dedup pass is
/// exercised. Run via <c>BENCH_MICROBENCH_SUITE=crosstree</c> (or
/// <c>--suite crosstree</c>); see <c>Program.cs</c>. No Orleans silo is in the
/// loop, so the <c>Allocated</c> column is deterministic.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrossTreeCanonicalSetBenchmarks
{
    /// <summary>Number of distinct participants in the barrier before duplication.</summary>
    [Params(2, 8, 64)]
    public int SetSize { get; set; }

    private string[] _waitSet = null!;
    private (string TreeId, int Payload)[] _participants = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Realistic tree-ids in a deliberately unsorted order, each repeated once
        // so the de-duplication pass has real work (the frozen sets in production
        // are formed from readiness/terminal reports that can double-count a tree).
        var ids = new List<string>(SetSize * 2);
        for (var i = SetSize - 1; i >= 0; i--)
        {
            ids.Add($"tree-{i:D4}");
        }
        for (var i = 0; i < SetSize; i++)
        {
            ids.Add($"tree-{i:D4}");
        }

        _waitSet = ids.ToArray();
        _participants = ids.Select(id => (TreeId: id, Payload: id.Length)).ToArray();
    }

    // ---- (A) wait-set freeze: Distinct().OrderBy().ToList() vs helper ----

    [Benchmark(Baseline = true)]
    public List<string> Baseline_List() =>
        _waitSet
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();

    [Benchmark]
    public List<string> Optimized_List() => CanonicalStringSet.SortedDistinct(_waitSet);

    // ---- (B) participant set: Select().Distinct().OrderBy().ToArray() vs helper ----

    [Benchmark]
    public string[] Baseline_Array() =>
        _participants
            .Select(p => p.TreeId)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToArray();

    [Benchmark]
    public string[] Optimized_Array() =>
        CanonicalStringSet.SortedDistinctArray(_participants.Select(p => p.TreeId));
}
