using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state optimizations made to the view-maintainer
/// drain fold (<c>Orleans.Lattice</c>) and the shared metrics sampler's per-tick
/// work (<c>Orleans.Lattice.Api.State</c>) so their per-operation byte and time
/// deltas are measurable in the clear.
/// <para>
/// As with the sibling <see cref="DrainAllocationTrimBenchmarks"/> suite, the
/// end-to-end cluster benchmarks route every operation through Orleans
/// serialization, persistence and task machinery and allocate on the order of
/// tens of kilobytes per op, so a sub-kilobyte trim sits below their run-to-run
/// noise floor. Each pair below reproduces exactly one prior code shape against
/// its optimized replacement with no cluster in the loop, so the
/// <c>Allocated</c> column is deterministic and the baseline-vs-optimized delta
/// is precisely the heap and CPU the production change removes.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the drain's per-batch fold - the prior form ran
/// <see cref="ViewKeyCollisionDetector.Detect(IEnumerable{ViewWrite})"/> and
/// <see cref="ViewWriteCoalescer.Coalesce(IEnumerable{ViewWrite})"/> as two
/// consecutive passes over the same batch, building two per-key dictionaries and
/// a colliding <c>HashSet</c> and hashing every key twice;
/// <see cref="ViewBatchFold.Fold(IReadOnlyList{ViewWrite})"/> folds both into one
/// pass over one dictionary with a single hash probe per write. This lane calls
/// the <b>real production code on both sides</b> - the two helpers remain public
/// API and are unchanged - so the delta is not a reconstruction;
/// (2) <c>SharedMetricsSampler.SampleAllAsync</c>'s per-tick de-duplication of an
/// explicit tree-id scope - the prior form used
/// <c>Distinct(StringComparer.Ordinal).ToList()</c>, which allocates the LINQ
/// iterator, its internal set, and a list grown from empty (a deferred iterator
/// gives <c>ToList</c> no count hint); the optimized form folds directly into a
/// presized list, scanning ordinally for the short scopes a dashboard actually
/// requests;
/// (3) the same tick's two dictionary folds - <c>SampleViewLagAsync</c>'s rollup
/// accumulation (a <c>TryGetValue</c> followed by an indexer set: two hash probes
/// per view) and <c>ResolveViewLag</c>'s two same-key <c>TryGetValue</c> calls
/// reading two fields off one struct - each collapsed to a single probe.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=fusiontrims</c> (or
/// <c>--suite fusiontrims</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ViewFoldAndMetricsTrimBenchmarks
{
    // ---- (1) a realistic drain batch: 256 writes over 64 distinct view keys,
    //      every write attributed to its own injective source key, so the common
    //      collision-free path is what is measured ----
    private List<ViewWrite> _batch = null!;

    // ---- (2) an explicit dashboard tree-id scope, with the duplicates a caller
    //      naturally sends when it unions several watch lists ----
    private List<string> _treeIds = null!;

    // ---- (3) one metrics tick's worth of view rows to roll up, then read back ----
    private string[] _viewSourceTreeIds = null!;
    private long?[] _viewLags = null!;
    private string[] _lookupTreeIds = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        const int batch = 256;
        const int distinctKeys = 64;

        _batch = new List<ViewWrite>(batch);
        for (var i = 0; i < batch; i++)
        {
            _batch.Add(ViewWrite.Upsert(
                "view-key-" + (i % distinctKeys).ToString("D4"),
                [(byte)i],
                new HybridLogicalClock { WallClockTicks = i, Counter = 0 },
                sourceKey: "source-key-" + i.ToString("D4")));
        }

        // A dashboard watching 12 trees, where unioning two overlapping scopes
        // left 4 duplicates behind - the shape the per-tick dedup actually sees.
        _treeIds = new List<string>(16);
        for (var i = 0; i < 12; i++)
        {
            _treeIds.Add("tree-" + i.ToString("D2"));
        }

        for (var i = 0; i < 4; i++)
        {
            _treeIds.Add("tree-" + i.ToString("D2"));
        }

        // 512 views spread over 32 source trees: the rollup fold walks every row,
        // then one lookup per tree reads the result back.
        _viewSourceTreeIds = new string[512];
        _viewLags = new long?[512];
        for (var i = 0; i < 512; i++)
        {
            _viewSourceTreeIds[i] = "tree-" + (i % 32).ToString("D2");
            _viewLags[i] = i % 5 == 0 ? null : i;
        }

        _lookupTreeIds = new string[32];
        for (var i = 0; i < 32; i++)
        {
            _lookupTreeIds[i] = "tree-" + i.ToString("D2");
        }
    }

    // ------------------------------------------------------------------
    // (1) drain fold: two passes vs one
    // ------------------------------------------------------------------

    /// <summary>
    /// The prior drain shape: detect collisions in one pass, then coalesce in a
    /// second pass over the same batch. Two per-key dictionaries, one colliding
    /// set, and two hash probes per write.
    /// </summary>
    [Benchmark(Baseline = true, Description = "(1) drain fold: Detect + Coalesce (two passes)")]
    public int DrainFold_TwoPasses()
    {
        var collisions = ViewKeyCollisionDetector.Detect(_batch);
        var survivors = ViewWriteCoalescer.Coalesce(_batch);
        return collisions.Count + survivors.Count;
    }

    /// <summary>
    /// The optimized drain shape: one pass, one dictionary of per-key slots, one
    /// hash probe per write, no colliding set.
    /// </summary>
    [Benchmark(Description = "(1) drain fold: ViewBatchFold.Fold (one pass)")]
    public int DrainFold_Fused()
    {
        var fold = ViewBatchFold.Fold(_batch);
        return fold.Collisions.Count + fold.Survivors.Count;
    }

    // ------------------------------------------------------------------
    // (2) per-tick tree-id de-duplication
    // ------------------------------------------------------------------

    /// <summary>
    /// The prior per-tick dedup: a LINQ <c>Distinct</c> iterator, its internal
    /// set, and a list grown from empty.
    /// </summary>
    [Benchmark(Description = "(2) tree-id dedup: Distinct().ToList()")]
    public int TreeIdDedup_Linq()
    {
        return _treeIds.Distinct(StringComparer.Ordinal).ToList().Count;
    }

    /// <summary>
    /// The optimized per-tick dedup: a direct fold into a presized list, ordinal
    /// scan for the short scopes a dashboard requests.
    /// </summary>
    [Benchmark(Description = "(2) tree-id dedup: presized ordinal fold")]
    public int TreeIdDedup_Fold()
    {
        return DistinctOrdinal(_treeIds).Count;
    }

    /// <summary>
    /// Mirrors the production helper: preserves first-seen order, presizes to the
    /// input count, and scans ordinally below the small-scope threshold.
    /// </summary>
    private static List<string> DistinctOrdinal(IReadOnlyList<string> source)
    {
        const int smallRequestThreshold = 16;
        var distinct = new List<string>(source.Count);

        if (source.Count <= smallRequestThreshold)
        {
            for (var i = 0; i < source.Count; i++)
            {
                var id = source[i];
                var seen = false;
                for (var j = 0; j < distinct.Count; j++)
                {
                    if (string.Equals(distinct[j], id, StringComparison.Ordinal))
                    {
                        seen = true;
                        break;
                    }
                }

                if (!seen)
                {
                    distinct.Add(id);
                }
            }

            return distinct;
        }

        var seenIds = new HashSet<string>(source.Count, StringComparer.Ordinal);
        for (var i = 0; i < source.Count; i++)
        {
            if (seenIds.Add(source[i]))
            {
                distinct.Add(source[i]);
            }
        }

        return distinct;
    }

    // ------------------------------------------------------------------
    // (3) metrics-tick dictionary folds: two probes vs one
    // ------------------------------------------------------------------

    /// <summary>Mirrors the production <c>ViewRollup</c> accumulator.</summary>
    private readonly record struct Rollup(int Count, long? LagTotal);

    /// <summary>
    /// The prior tick shape: <c>TryGetValue</c> then an indexer set while rolling
    /// up (two probes per view), then two same-key <c>TryGetValue</c> calls per
    /// tree to read two fields off one struct.
    /// </summary>
    [Benchmark(Description = "(3) metrics tick folds: two hash probes per row")]
    public long MetricsTick_DoubleProbe()
    {
        var rollups = new Dictionary<string, Rollup>(StringComparer.Ordinal);
        for (var i = 0; i < _viewSourceTreeIds.Length; i++)
        {
            rollups.TryGetValue(_viewSourceTreeIds[i], out var current);
            rollups[_viewSourceTreeIds[i]] = new Rollup(
                current.Count + 1,
                _viewLags[i] is { } lag ? (current.LagTotal ?? 0) + lag : current.LagTotal);
        }

        long total = 0;
        for (var i = 0; i < _lookupTreeIds.Length; i++)
        {
            var count = rollups.TryGetValue(_lookupTreeIds[i], out var rollup) ? rollup.Count : 0;
            var lagTotal = rollups.TryGetValue(_lookupTreeIds[i], out var lag) ? lag.LagTotal : null;
            total += count + (lagTotal ?? 0);
        }

        return total;
    }

    /// <summary>
    /// The optimized tick shape: one <c>GetValueRefOrAddDefault</c> probe per
    /// view row, and one <c>TryGetValue</c> per tree serving both fields.
    /// </summary>
    [Benchmark(Description = "(3) metrics tick folds: single hash probe per row")]
    public long MetricsTick_SingleProbe()
    {
        var rollups = new Dictionary<string, Rollup>(StringComparer.Ordinal);
        for (var i = 0; i < _viewSourceTreeIds.Length; i++)
        {
            ref var rollup = ref CollectionsMarshal.GetValueRefOrAddDefault(
                rollups, _viewSourceTreeIds[i], out _);
            rollup = new Rollup(
                rollup.Count + 1,
                _viewLags[i] is { } lag ? (rollup.LagTotal ?? 0) + lag : rollup.LagTotal);
        }

        long total = 0;
        for (var i = 0; i < _lookupTreeIds.Length; i++)
        {
            var (count, lagTotal) = rollups.TryGetValue(_lookupTreeIds[i], out var rollup)
                ? (rollup.Count, rollup.LagTotal)
                : (0, null);
            total += count + (lagTotal ?? 0);
        }

        return total;
    }
}
