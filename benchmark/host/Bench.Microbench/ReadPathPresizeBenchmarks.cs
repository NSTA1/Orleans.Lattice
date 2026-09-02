using System;
using System.Collections.Generic;
using System.Globalization;

using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state result-list presize trims shipped in this
/// change so their per-operation byte deltas are measurable in the clear. As
/// with the sibling <see cref="ReadPathAllocationBenchmarks"/> suite, the full
/// end-to-end cluster benchmarks route each read through Orleans serialization,
/// persistence, and task machinery and allocate on the order of tens of
/// kilobytes per op, so a sub-kilobyte trim sits below their run-to-run noise
/// floor. Each benchmark below reproduces exactly one optimized code shape
/// against its prior shape with no cluster in the loop, so the <c>Allocated</c>
/// column is deterministic and the baseline-vs-optimized delta is precisely the
/// heap the production change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>BPlusLeafGrain.GetEntriesAsync</c> - the prior form grew the result
/// list from empty while scanning the leaf's ordered cache rows; presizing it to
/// <c>Math.Min(Cache.Count, 256)</c> (a tight upper bound on the emitted count,
/// exactly as the sibling <c>GetKeysAsync</c> already did) removes the small-end
/// resize chain;
/// (2) <c>LatticeStateQuery.GetTreeStructureAsync</c> - the prior form grew the
/// top-level <c>rootNodes</c> list from empty across the scanned shard span;
/// presizing it to that span (one summary per shard, the budget-capped upper
/// bound) removes the regrowth;
/// (3) <c>CausalApplyBuffer.DrainSatisfied</c> - the prior form grew the drained
/// <c>ready</c> list from empty while walking the parked-entry list; presizing it
/// to the parked-entry count (the drain's upper bound) removes the regrowth.
/// </para>
/// <para>
/// The shapes are reproduced against stand-in record types (the production value
/// types are Orleans-serializable records whose own size is identical between
/// each pair's lanes, so the reported delta is exactly the collection overhead
/// the edit removes, not the payload). Run it via
/// <c>BENCH_MICROBENCH_SUITE=readpathpresize</c> (or <c>--suite readpathpresize</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast
/// to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence
/// intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReadPathPresizeBenchmarks
{
    /// <summary>A minimal stand-in for a leaf cache row value.</summary>
    private readonly record struct Row(byte[] Value, bool IsTombstone);

    /// <summary>A minimal stand-in for the internal <c>NodeStateSummary</c> record.</summary>
    private sealed record NodeSummary(int ShardIndex, string Label);

    /// <summary>A minimal stand-in for a parked <c>WalRecord</c>.</summary>
    private sealed record Parked(string Key, byte[] Payload);

    // ---- (1) the ordered cache rows a range-entries read folds ----
    private KeyValuePair<string, Row>[] _cacheRows = null!;
    private int _cacheCount;

    // ---- (2) the scanned shard span a tree-structure read emits over ----
    private int _startShard;
    private int _endShard;

    // ---- (3) the parked entries a causal drain walks ----
    private LinkedList<Parked> _parked = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A full leaf's worth of live cache rows (no tombstones => every row is
        // emitted, the fold's upper bound), so Math.Min(Cache.Count, 256) caps at
        // 256 and models the common ~250-entry-per-leaf shape.
        const int rowCount = 512;
        _cacheRows = new KeyValuePair<string, Row>[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var key = "key-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _cacheRows[i] = new KeyValuePair<string, Row>(key, new Row(new byte[8], IsTombstone: false));
        }

        _cacheCount = rowCount;

        // A whole-tree structure scan over a moderate shard count: one root
        // summary per shard, none truncated by the budget.
        _startShard = 0;
        _endShard = 64;

        // A causal-apply buffer holding a run of parked entries that a single
        // version-vector advance unblocks in full (the typical drain).
        const int parkedCount = 128;
        _parked = new LinkedList<Parked>();
        for (var i = 0; i < parkedCount; i++)
        {
            _parked.AddLast(new Parked("dep-" + i.ToString("D4", CultureInfo.InvariantCulture), new byte[8]));
        }
    }

    // ------------------------------------------------------------------
    // (1) BPlusLeafGrain.GetEntriesAsync result list
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the range-entries list grows from empty as the cache scan
    /// populates it, reallocating its backing array up the small-end chain.
    /// </summary>
    [Benchmark(Baseline = true, Description = "GetEntries: list grow from empty (baseline)")]
    public int GetEntries_GrowFromEmpty()
    {
        var entries = new List<KeyValuePair<string, byte[]>>();
        foreach (var (key, row) in _cacheRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            entries.Add(new KeyValuePair<string, byte[]>(key, row.Value));
        }

        return entries.Count;
    }

    /// <summary>
    /// Optimized: presizing the list to <c>Math.Min(Cache.Count, 256)</c> (a
    /// tight upper bound on the emitted count) removes the regrowth; the scan body
    /// is identical.
    /// </summary>
    [Benchmark(Description = "GetEntries: list presized (optimized)")]
    public int GetEntries_Presized()
    {
        var entries = new List<KeyValuePair<string, byte[]>>(capacity: Math.Min(_cacheCount, 256));
        foreach (var (key, row) in _cacheRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            entries.Add(new KeyValuePair<string, byte[]>(key, row.Value));
        }

        return entries.Count;
    }

    // ------------------------------------------------------------------
    // (2) LatticeStateQuery.GetTreeStructureAsync rootNodes list
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the top-level root-nodes list grows from empty as the shard scan
    /// appends one summary per shard, reallocating its backing array.
    /// </summary>
    [Benchmark(Description = "TreeStructure: rootNodes grow from empty (baseline)")]
    public int TreeStructure_GrowFromEmpty()
    {
        var rootNodes = new List<NodeSummary>();
        for (var shardIndex = _startShard; shardIndex < _endShard; shardIndex++)
        {
            rootNodes.Add(new NodeSummary(shardIndex, "root"));
        }

        return rootNodes.Count;
    }

    /// <summary>
    /// Optimized: presizing the list to the scanned shard span (one summary per
    /// shard, the budget-capped upper bound) removes the regrowth; the scan body
    /// is identical.
    /// </summary>
    [Benchmark(Description = "TreeStructure: rootNodes presized (optimized)")]
    public int TreeStructure_Presized()
    {
        var rootNodes = new List<NodeSummary>(Math.Max(0, _endShard - _startShard));
        for (var shardIndex = _startShard; shardIndex < _endShard; shardIndex++)
        {
            rootNodes.Add(new NodeSummary(shardIndex, "root"));
        }

        return rootNodes.Count;
    }

    // ------------------------------------------------------------------
    // (3) CausalApplyBuffer.DrainSatisfied ready list
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the drained list grows from empty as the parked-entry walk
    /// collects every satisfied record, reallocating its backing array.
    /// </summary>
    [Benchmark(Description = "DrainSatisfied: ready grow from empty (baseline)")]
    public int DrainSatisfied_GrowFromEmpty()
    {
        var ready = new List<Parked>();
        for (var node = _parked.First; node is not null; node = node.Next)
        {
            ready.Add(node.Value);
        }

        return ready.Count;
    }

    /// <summary>
    /// Optimized: presizing the list to the parked-entry count (the drain's upper
    /// bound) removes the regrowth; the walk body is identical.
    /// </summary>
    [Benchmark(Description = "DrainSatisfied: ready presized (optimized)")]
    public int DrainSatisfied_Presized()
    {
        var ready = new List<Parked>(_parked.Count);
        for (var node = _parked.First; node is not null; node = node.Next)
        {
            ready.Add(node.Value);
        }

        return ready.Count;
    }
}
