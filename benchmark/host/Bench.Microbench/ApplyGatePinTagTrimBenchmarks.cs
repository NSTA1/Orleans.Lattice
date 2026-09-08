using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three allocation reductions on per-operation paths, so the byte
/// and time deltas are measurable in the clear rather than buried under a
/// silo, a transport, and a storage provider.
/// <para>
/// (1) <c>ReplicationApplier.BuildParallelApplyPlanOrNull</c>, run once per
/// inbound replication batch. Parallel apply is opt-in and
/// <c>ApplyMaxParallelRuns</c> defaults to <c>1</c>, but the prior shape
/// resolved that option only <i>after</i> materialising the whole per-tree
/// run-segment grouping - a dictionary, one list per participating tree, a
/// list of tree ids, and the full segmentation walk - and then discarded all
/// of it, leaving the sequential path to re-derive the identical boundaries.
/// The replacement resolves the gate in an allocation-free scan first, so the
/// default posture allocates nothing. The optimized lane calls the
/// <b>real production code</b>.
/// </para>
/// <para>
/// (2) <c>LeafCursorReporter</c>'s durable-pin bucketing, run once per
/// checkpoint flush and once per leaf birth. The prior shape grew every
/// per-shard bucket list from empty through the 4/8/16/... doubling chain,
/// abandoning each intermediate array. The replacement presizes each bucket
/// from a genuine numerator over a genuine divisor - the report count over the
/// shard/bucket width those reports route to. The optimized lane calls the
/// <b>real production</b> <c>GroupByBucketSlot</c>. A third contrast lane
/// measures the <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>
/// probe fold that was <i>not</i> shipped, so the decision to ship the presize
/// alone is evidenced rather than asserted.
/// </para>
/// <para>
/// (3) <c>LatticeTagIndexContext.ReconcileTagSet</c>, run once per
/// tag-carrying write. The prior shape copied the current tag list into a
/// <c>HashSet</c> purely to answer a handful of <c>Contains</c> probes, left
/// the desired set unsized despite holding an exact upper bound, and allocated
/// both partition lists unconditionally - including in the converged
/// "tags unchanged" steady state, where both are empty. The replacement
/// presizes, answers membership with a linear ordinal scan at small widths,
/// and allocates each partition list lazily. The optimized lane calls the
/// <b>real production code</b>.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=applygatetrims</c> (or
/// <c>--suite applygatetrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ApplyGatePinTagTrimBenchmarks
{
    // ---- (1) one inbound multi-tree replication batch on the default
    //      (fully sequential) apply posture ----
    private WalRecord[] _multiTreeBatch = null!;
    private IOptionsMonitor<LatticeReplicationOptions> _replicationOptions = null!;

    // ---- (2) one leaf's per-checkpoint pin flush across the default pin
    //      fan-out width ----
    private MaterialiserPinReport[] _pinReports = null!;
    private const int PinBucketCount = 8;

    // ---- (3) one tag-carrying write's reconcile, in the converged steady
    //      state (the tags did not change) ----
    private HashSet<string> _desiredTags = null!;
    private string[] _currentTags = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // (1) A drained inbound batch spanning eight trees, in contiguous runs
        // as the shipper produces them. Multi-tree is what makes the prior
        // shape allocate; the default ApplyMaxParallelRuns of 1 is what makes
        // every one of those allocations garbage.
        const int treeCount = 8;
        const int perTree = 64;
        _multiTreeBatch = new WalRecord[treeCount * perTree];
        var payload = new byte[64];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)i;
        }

        var slot = 0;
        for (var t = 0; t < treeCount; t++)
        {
            for (var i = 0; i < perTree; i++)
            {
                _multiTreeBatch[slot] = new WalRecord
                {
                    TreeId = "tree-" + t.ToString("D2"),
                    Op = MutationKind.Set,
                    Key = "customer/" + slot.ToString("D6"),
                    Value = payload,
                    Timestamp = new HybridLogicalClock
                    {
                        WallClockTicks = 638_000_000_000_000_000L + slot,
                        Counter = 0,
                    },
                    // Two origins per tree, alternating in contiguous runs, so
                    // the segmentation walk the prior shape performed has real
                    // run boundaries to find rather than degenerating to one.
                    OriginClusterId = (i / 16) % 2 == 0 ? "region-a" : "region-b",
                };
                slot++;
            }
        }

        _replicationOptions = new DefaultReplicationOptionsMonitor();

        // (2) 64 partition-consumers reporting into the default 8-way pin
        // fan-out: a real numerator over a real divisor, so the presize is
        // eight reports per bucket rather than the doubling chain.
        _pinReports = new MaterialiserPinReport[64];
        for (var i = 0; i < _pinReports.Length; i++)
        {
            _pinReports[i] = new MaterialiserPinReport(
                "leaf/" + i.ToString("D4") + "/consumer",
                new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + i,
                    Counter = 0,
                },
                4096L * i);
        }

        // (3) Six tags on the key - a realistic width, and comfortably inside
        // the linear-scan threshold - re-written unchanged, which is the
        // steady state a repeated Set with the same tags produces.
        _currentTags =
        [
            "env:prod",
            "team:payments",
            "tier:gold",
            "region:westeurope",
            "pii:false",
            "retention:7y",
        ];
        _desiredTags = new HashSet<string>(_currentTags, StringComparer.Ordinal);
    }

    // ========================================================================
    // (1) per-batch parallel-apply plan gating
    // ========================================================================

    /// <summary>
    /// The prior shape: materialise the whole per-tree run-segment grouping,
    /// then resolve <c>ApplyMaxParallelRuns</c> and discard it. Reproduced
    /// here because the production method no longer contains it; the surrounding
    /// shell (the same <c>IReadOnlyList&lt;WalRecord&gt;</c> indexing, the same
    /// single-tree pre-check, the same options monitor) is identical to the
    /// optimized lane so the delta is only the ordering of the gate.
    /// </summary>
    [Benchmark]
    public int ApplyPlanGate_Baseline_GroupThenDiscard()
    {
        IReadOnlyList<WalRecord> entries = _multiTreeBatch;

        var firstTree = entries[0].TreeId;
        var multiTree = false;
        for (var k = 1; k < entries.Count; k++)
        {
            if (!string.Equals(entries[k].TreeId, firstTree, StringComparison.Ordinal))
            {
                multiTree = true;
                break;
            }
        }
        if (!multiTree)
        {
            return 0;
        }

        var groups = new Dictionary<string, List<(int Start, int End)>>(StringComparer.Ordinal);
        var order = new List<string>();
        var i = 0;
        while (i < entries.Count)
        {
            var end = FindRunEndExclusive(entries, i);
            var treeId = entries[i].TreeId ?? string.Empty;
            if (!groups.TryGetValue(treeId, out var runs))
            {
                runs = [];
                groups[treeId] = runs;
                order.Add(treeId);
            }

            runs.Add((i, end));
            i = end;
        }

        var maxParallel = 1;
        foreach (var treeId in order)
        {
            var configured = _replicationOptions.Get(treeId).ApplyMaxParallelRuns;
            if (configured > maxParallel)
            {
                maxParallel = configured;
            }
        }

        return maxParallel <= 1 || order.Count <= 1 ? 0 : order.Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>ReplicationApplier.BuildParallelApplyPlanOrNull</c>, which resolves
    /// the gate in an allocation-free scan before materialising anything.
    /// </summary>
    [Benchmark]
    public int ApplyPlanGate_Optimized_GateBeforeGrouping()
    {
        var plan = ReplicationApplier.BuildParallelApplyPlanOrNull(_multiTreeBatch, _replicationOptions);
        return plan?.TreeOrder.Count ?? 0;
    }

    /// <summary>
    /// Run-boundary walk reproduced for the baseline lane only, matching the
    /// production segmentation exactly.
    /// </summary>
    private static int FindRunEndExclusive(IReadOnlyList<WalRecord> entries, int start)
    {
        var head = entries[start];
        var treeId = head.TreeId;
        var origin = head.OriginClusterId;
        var i = start + 1;
        while (i < entries.Count)
        {
            var candidate = entries[i];
            if (!string.Equals(candidate.TreeId, treeId, StringComparison.Ordinal)
                || !string.Equals(candidate.OriginClusterId, origin, StringComparison.Ordinal))
            {
                break;
            }

            i++;
        }

        return i;
    }

    // ========================================================================
    // (2) per-checkpoint durable-pin bucketing
    // ========================================================================

    /// <summary>
    /// The prior shape: every per-bucket list grows from empty through the
    /// 4/8/16 doubling chain, abandoning each intermediate array.
    /// </summary>
    [Benchmark]
    public int PinBucketing_Baseline_GrowFromEmpty()
    {
        IReadOnlyList<MaterialiserPinReport> reports = _pinReports;
        var grouped = new Dictionary<string, List<MaterialiserPinReport>>(StringComparer.Ordinal);
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            var slot = WalMaterialiserPinRouting.BucketStateName(report.ConsumerId, PinBucketCount);
            if (!grouped.TryGetValue(slot, out var list))
            {
                list = [];
                grouped[slot] = list;
            }

            list.Add(report);
        }

        return grouped.Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>LeafCursorReporter.GroupByBucketSlot</c>, presizing each bucket from
    /// the report count over the bucket width.
    /// </summary>
    [Benchmark]
    public int PinBucketing_Optimized_ShardFairPresize()
        => LeafCursorReporter.GroupByBucketSlot(_pinReports, PinBucketCount).Count;

    /// <summary>
    /// Contrast lane for the alternative that was <b>not</b> shipped: folding
    /// the probe onto a single
    /// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>
    /// on top of the presize. It removes one hash per report but no
    /// allocation, so it is measured rather than bundled in unmeasured.
    /// </summary>
    [Benchmark]
    public int PinBucketing_Contrast_PresizePlusProbeFold()
    {
        IReadOnlyList<MaterialiserPinReport> reports = _pinReports;
        var grouped = new Dictionary<string, List<MaterialiserPinReport>>(StringComparer.Ordinal);
        var capacity = Math.Max(4, reports.Count / Math.Max(1, Math.Min(PinBucketCount, reports.Count)));
        for (var i = 0; i < reports.Count; i++)
        {
            var report = reports[i];
            var slot = WalMaterialiserPinRouting.BucketStateName(report.ConsumerId, PinBucketCount);
            ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(grouped, slot, out _);
            list ??= new List<MaterialiserPinReport>(capacity);
            list.Add(report);
        }

        return grouped.Count;
    }

    // ========================================================================
    // (3) per-write tag reconcile
    // ========================================================================

    /// <summary>
    /// The prior shape: copy the current tag list into a <c>HashSet</c> purely
    /// to answer <c>Contains</c>, and allocate both partition lists whether or
    /// not anything changed.
    /// </summary>
    [Benchmark]
    public int TagReconcile_Baseline_SetCopyAndEagerLists()
    {
        IReadOnlyList<string> current = _currentTags;
        var currentSet = new HashSet<string>(current, StringComparer.Ordinal);

        var toAdd = new List<string>();
        foreach (var tag in _desiredTags)
        {
            if (!currentSet.Contains(tag))
            {
                toAdd.Add(tag);
            }
        }

        var toRemove = new List<string>();
        foreach (var tag in currentSet)
        {
            if (!_desiredTags.Contains(tag))
            {
                toRemove.Add(tag);
            }
        }

        return toAdd.Count + toRemove.Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>LatticeTagIndexContext.ReconcileTagSet</c>, which answers membership
    /// with a linear ordinal scan and allocates each partition list lazily, so
    /// the converged case allocates nothing at all.
    /// </summary>
    [Benchmark]
    public int TagReconcile_Optimized_ScanAndLazyLists()
    {
        LatticeTagIndexContext.ReconcileTagSet(_desiredTags, _currentTags, out var toAdd, out var toRemove);
        return (toAdd?.Count ?? 0) + (toRemove?.Count ?? 0);
    }

    /// <summary>
    /// Fixed options monitor standing in for the default replication posture:
    /// every tree resolves the shipped defaults, so
    /// <c>ApplyMaxParallelRuns</c> is <c>1</c> and parallel apply is off.
    /// </summary>
    private sealed class DefaultReplicationOptionsMonitor : IOptionsMonitor<LatticeReplicationOptions>
    {
        private readonly LatticeReplicationOptions _value = new();

        public LatticeReplicationOptions CurrentValue => _value;

        public LatticeReplicationOptions Get(string? name) => _value;

        public IDisposable? OnChange(Action<LatticeReplicationOptions, string?> listener) => null;
    }
}
