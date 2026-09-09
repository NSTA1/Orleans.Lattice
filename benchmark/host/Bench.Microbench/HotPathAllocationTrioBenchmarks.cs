using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three allocation reductions on repeatedly-executed paths, so the
/// byte and time deltas are measurable in the clear rather than buried under a
/// silo, a transport, and a storage provider.
/// <para>
/// (1) <c>SharedMetricsSampler.SampleAllAsync</c>'s per-tick aggregate map, one
/// rebuild per sampling tick per distinct subscriber signature, fanned to every
/// attached dashboard. The prior shape grew a <c>Dictionary</c> from empty
/// through the whole 3/7/17/37/... doubling chain even though the tree-id list
/// it folds is already materialised and already ordinally de-duplicated one
/// statement earlier, so an exact upper bound on the final size was in hand and
/// unused. The shipped shape hints <c>treeIds.Count</c>. A third contrast lane
/// measures the <i>guessed</i> constant hint that was deliberately not shipped,
/// so the "derive the bound, never estimate it" rule is evidenced rather than
/// asserted.
/// </para>
/// <para>
/// (2) <c>CompiledTenantUsage.Compile</c>, run once per tenant-registry or
/// tenant-usage tree change to rebuild the warm write-admission snapshot. Both
/// of its accumulators grew from empty. The signature takes
/// <c>IEnumerable&lt;T&gt;</c> and cannot widen without breaking the public
/// surface, but the only production caller - <c>TenantUsageIndexMaintainer</c> -
/// always materialises both scans into <c>List&lt;T&gt;</c> first, so an
/// <c>ICollection&lt;T&gt;</c> count probe recovers a real exact upper bound at
/// no API cost. The optimized lane calls the <b>real production code</b>.
/// </para>
/// <para>
/// (3) <c>LatticeStatsGrain.BuildReportAsync</c>'s shard-report ordering, run
/// once per diagnostics report. The physical shard indices the fan-out walks are
/// already ascending and the dispatch preserves that order positionally, so the
/// prior <c>OrderBy(...).ToImmutableArray()</c> paid a full LINQ ordering
/// pipeline - the ordered enumerable, a buffered copy of the source, the
/// projected key array and the sort map - plus a second copy into the immutable
/// array, purely to reproduce the order it already had. The shipped shape
/// verifies the order in one allocation-free scan and hands the (method-local,
/// never aliased) array straight to the immutable wrapper. The optimized lane
/// calls the <b>real production</b> <c>SortByShardIndexIfNeeded</c>. A third
/// contrast lane measures dropping the LINQ <i>without</i> the zero-copy wrap,
/// so the wrap's contribution is separated from the ordering removal's.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=alloctrio</c> (or
/// <c>--suite alloctrio</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class HotPathAllocationTrioBenchmarks
{
    // ---- (1) one shared-sampler tick over a dashboard-sized tree scope ----
    private const int SampledTreeCount = 48;
    private List<string> _treeIds = null!;
    private TreeMetrics[] _treeMetrics = null!;

    // The unshipped guess: the sampler's own small-request threshold, which is a
    // plausible-looking constant and a lower bound on this scope, not an upper
    // one. Present only to evidence why the derived count was shipped instead.
    private const int GuessedTreeCapacity = 16;

    // ---- (2) one admission-snapshot rebuild over a mid-sized tenant estate ----
    private const int TenantCount = 64;
    private List<TenantRecord> _registry = null!;
    private List<TenantUsageRecord> _usage = null!;
    private const string LocalClusterId = "cluster-local";

    // ---- (3) one diagnostics report over a fanned-out tree, shards already
    //      ascending as the physical shard walk emits them ----
    private const int ShardCount = 64;
    private ShardDiagnosticReport[] _shardTemplate = null!;

    /// <summary>Builds the inputs shared by the benchmark groups.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // (1) A dashboard scope naming more trees than any fixed guess would
        // cover, matching the catalog enumeration the sampler folds per tick.
        _treeIds = new List<string>(SampledTreeCount);
        _treeMetrics = new TreeMetrics[SampledTreeCount];
        for (var i = 0; i < SampledTreeCount; i++)
        {
            var treeId = "tree-" + i.ToString("D3");
            _treeIds.Add(treeId);
            _treeMetrics[i] = new TreeMetrics
            {
                TreeId = treeId,
                Lifecycle = TreeLifecycleState.Active,
                ShardCount = 8,
                ViewCount = 2,
                ShardHotness = Array.Empty<Api.State.ShardHotness>(),
            };
        }

        // (2) A registry with a matching usage record per tenant, exactly as the
        // maintainer's two scans hand them over: concrete lists, one entry per
        // tenant, every registry record joined.
        var clock = new HybridLogicalClock();
        _registry = new List<TenantRecord>(TenantCount);
        _usage = new List<TenantUsageRecord>(TenantCount);
        for (var i = 0; i < TenantCount; i++)
        {
            var tenant = TenantId.Parse("tenant-" + i.ToString("D3"));
            _registry.Add(TenantRecord.Create(
                tenant,
                TenantStatus.Active,
                TenantQuotas.Unbounded,
                TenantPlacement.Shared,
                clock,
                writerId: null));
            _usage.Add(TenantUsageRecord.Create(tenant));
        }

        // (3) Reports in the ascending shard order the production fan-out
        // produces, which is the order the removed OrderBy was re-deriving.
        _shardTemplate = new ShardDiagnosticReport[ShardCount];
        for (var i = 0; i < ShardCount; i++)
        {
            _shardTemplate[i] = new ShardDiagnosticReport
            {
                ShardIndex = i,
                Depth = 3,
                LiveKeys = 1024 + i,
                Tombstones = i,
                OpsPerSecond = i,
                Reads = 10L * i,
                Writes = 5L * i,
                HotnessWindow = TimeSpan.FromSeconds(30),
            };
        }
    }

    // ========================================================================
    // (1) shared metrics sampler per-tick aggregate map
    // ========================================================================

    /// <summary>
    /// The prior shape: the per-tick map grows from empty. Reproduced here
    /// because the production method no longer contains it; the surrounding
    /// shell (the same already-materialised tree-id list, the same ordinal
    /// comparer, the same one-entry-per-id fold) is identical to the optimized
    /// lane so the only difference is the capacity hint.
    /// </summary>
    [Benchmark]
    public int MetricsTick_Baseline_GrowFromEmpty()
    {
        var result = new Dictionary<string, TreeMetrics>(StringComparer.Ordinal);
        for (var i = 0; i < _treeIds.Count; i++)
        {
            result[_treeIds[i]] = _treeMetrics[i];
        }

        return result.Count;
    }

    /// <summary>
    /// The shipped shape: hint the exact upper bound the caller already holds.
    /// </summary>
    [Benchmark]
    public int MetricsTick_Optimized_PresizeFromTreeIdCount()
    {
        var result = new Dictionary<string, TreeMetrics>(_treeIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < _treeIds.Count; i++)
        {
            result[_treeIds[i]] = _treeMetrics[i];
        }

        return result.Count;
    }

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: a plausible-looking
    /// constant hint that is a lower bound rather than an upper one, so the
    /// doubling chain resumes from it instead of being eliminated.
    /// </summary>
    [Benchmark]
    public int MetricsTick_Contrast_GuessedConstantCapacity()
    {
        var result = new Dictionary<string, TreeMetrics>(GuessedTreeCapacity, StringComparer.Ordinal);
        for (var i = 0; i < _treeIds.Count; i++)
        {
            result[_treeIds[i]] = _treeMetrics[i];
        }

        return result.Count;
    }

    // ========================================================================
    // (2) tenant admission snapshot rebuild
    // ========================================================================

    /// <summary>
    /// The prior shape: both accumulators grow from empty. Reproduced here
    /// because the production method no longer contains it; every other step -
    /// the same argument guards, the same null and uninitialised-id skips, the
    /// same fold, the same local slot resolution, the same frozen-dictionary
    /// handoff - is identical to the optimized lane, so the only difference is
    /// the two capacity hints.
    /// </summary>
    [Benchmark]
    public int TenantCompile_Baseline_GrowFromEmpty()
    {
        IEnumerable<TenantRecord> registry = _registry;
        IEnumerable<TenantUsageRecord> usage = _usage;
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(usage);

        var usageById = new Dictionary<string, TenantUsageRecord>(StringComparer.Ordinal);
        foreach (var record in usage)
        {
            if (record is not null && record.Id.Value is { } id)
            {
                usageById[id] = record;
            }
        }

        var tenants = new Dictionary<string, TenantUsageView>(StringComparer.Ordinal);
        foreach (var record in registry)
        {
            if (record is null || record.Id.Value is not { } id)
            {
                continue;
            }

            LocalUsageSample global;
            LocalUsageSample local;
            if (usageById.TryGetValue(id, out var usageRecord))
            {
                global = usageRecord.Fold();
                local = usageRecord.LocalSample(LocalClusterId);
            }
            else
            {
                global = LocalUsageSample.Empty;
                local = LocalUsageSample.Empty;
            }

            tenants[id] = new TenantUsageView(record.Quotas, global, local);
        }

        return tenants.Count == 0
            ? 0
            : tenants.ToFrozenDictionary(StringComparer.Ordinal).Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>CompiledTenantUsage.Compile</c>, which recovers the exact upper bound
    /// through an <c>ICollection&lt;T&gt;</c> probe on each source.
    /// </summary>
    [Benchmark]
    public int TenantCompile_Optimized_RealCompile()
        => CompiledTenantUsage.Compile(_registry, _usage, LocalClusterId).TenantCount;

    // ========================================================================
    // (3) diagnostics shard-report ordering
    // ========================================================================

    /// <summary>
    /// The prior shape: order the already-ordered reports through LINQ, then
    /// copy the result into an immutable array. The shell (the same freshly
    /// allocated fan-out result array, the same ascending source order) is
    /// identical to the optimized lane.
    /// </summary>
    [Benchmark]
    public int ShardOrdering_Baseline_OrderByThenCopy()
    {
        var reports = CloneShardReports();
        var sorted = reports.OrderBy(s => s.ShardIndex).ToImmutableArray();
        return sorted.Length;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>LatticeStatsGrain.SortByShardIndexIfNeeded</c> scan, followed by the
    /// zero-copy immutable wrap the grain performs.
    /// </summary>
    [Benchmark]
    public int ShardOrdering_Optimized_ScanThenZeroCopyWrap()
    {
        var reports = CloneShardReports();
        LatticeStatsGrain.SortByShardIndexIfNeeded(reports);
        var sorted = ImmutableCollectionsMarshal.AsImmutableArray(reports);
        return sorted.Length;
    }

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: drop the LINQ ordering but
    /// keep the defensive copy into the immutable array, isolating how much of
    /// the win comes from the zero-copy wrap rather than from removing the
    /// ordering pipeline.
    /// </summary>
    [Benchmark]
    public int ShardOrdering_Contrast_ScanThenCopyingWrap()
    {
        var reports = CloneShardReports();
        LatticeStatsGrain.SortByShardIndexIfNeeded(reports);
        var sorted = ImmutableArray.Create(reports);
        return sorted.Length;
    }

    /// <summary>
    /// Reproduces the fan-out result array every lane starts from. Charged
    /// identically to all three lanes, so it cancels out of the comparison while
    /// keeping the zero-copy wrap honest: production hands the wrapper a fresh,
    /// method-local array, and so does this.
    /// </summary>
    private ShardDiagnosticReport[] CloneShardReports()
    {
        var reports = new ShardDiagnosticReport[ShardCount];
        Array.Copy(_shardTemplate, reports, ShardCount);
        return reports;
    }
}
