using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Shard-summary ordering on <c>LatticeStateQuery.GetShardSummariesAsync</c>,
/// which the shared metrics sampler runs once per tree on every tick.
/// <para>
/// <b>Judge this suite on Allocated.</b> The lane is sub-microsecond and the
/// benchmark host is shared, so Mean is only reported as a direction; the trim
/// is an allocation trim, and Allocated reproduces bit-for-bit.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> All three arms consume the identical pre-built
/// <see cref="ImmutableArray{T}"/> built in <see cref="Setup"/>, produce the
/// identical <c>ShardStateSummary[]</c>, and read one member back so nothing is
/// elided. The shipped arm calls the real shipped
/// <c>LatticeStateQuery.MapShardsByIndex</c> rather than a copy of it, so this
/// lane is its own production anchor.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=stateorder</c> (or
/// <c>--suite stateorder</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ShardSummaryOrderingBenchmarks
{
    /// <summary>
    /// Physical shards the report carries. <c>16</c> is a routine tree; <c>64</c>
    /// is a wide one, where the per-tick cost of the ordering shape is most
    /// visible.
    /// </summary>
    [Params(16, 64)]
    public int ShardCount { get; set; }

    private ImmutableArray<ShardDiagnosticReport> _shards;

    /// <summary>
    /// Builds the report rows. They are emitted in ascending shard order, which
    /// is the order a real diagnostic report arrives in, so every arm measures
    /// the already-ordered case the sampler actually pays for.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        var builder = ImmutableArray.CreateBuilder<ShardDiagnosticReport>(ShardCount);
        for (var i = 0; i < ShardCount; i++)
        {
            builder.Add(new ShardDiagnosticReport
            {
                ShardIndex = i,
                Depth = 3,
                RootIsLeaf = false,
                LiveKeys = 10_000 + i,
                Tombstones = i,
                TombstoneRatio = 0.01,
                OpsPerSecond = 12.5 + i,
                Reads = 1_000 + i,
                Writes = 500 + i,
                HotnessWindow = TimeSpan.FromSeconds(30),
                SplitInProgress = false,
                BulkOperationPending = false,
            });
        }

        _shards = builder.MoveToImmutable();
    }

    /// <summary>
    /// Baseline: the prior <c>OrderBy(...).Select(...).ToArray()</c> chain. It
    /// boxes the immutable array into its <c>IEnumerable&lt;T&gt;</c> parameter,
    /// buffers a full copy of the wide <see cref="ShardDiagnosticReport"/>
    /// structs, materialises a parallel key array and an index map, allocates
    /// the key-selector and projection delegates, an ordered enumerable and a
    /// projection iterator, and then grows a second buffer through its doubling
    /// chain before copying out.
    /// </summary>
    [Benchmark(Description = "Shard summaries: OrderBy + Select + ToArray (baseline)")]
    public long Shards_Baseline_LinqOrderBySelect()
    {
        var mapped = _shards
            .OrderBy(s => s.ShardIndex)
            .Select(s => new ShardStateSummary
            {
                ShardIndex = s.ShardIndex,
                Depth = s.Depth,
                RootIsLeaf = s.RootIsLeaf,
                LiveKeys = s.LiveKeys,
                Tombstones = s.Tombstones,
                OpsPerSecond = s.OpsPerSecond,
                SplitInProgress = s.SplitInProgress,
            })
            .ToArray();

        return mapped.Length == 0 ? 0 : mapped[^1].LiveKeys;
    }

    /// <summary>
    /// Shipped: the real <c>MapShardsByIndex</c> - one exact-width array, one
    /// wide-struct copy per row bound to a local, an ascending-detection flag
    /// that returns immediately on the ordered input the sampler always sees,
    /// and a stable insertion sort otherwise.
    /// </summary>
    [Benchmark(Description = "Shard summaries: exact-width map + ordered fast path (shipped)")]
    public long Shards_Shipped_MapByIndex()
    {
        var mapped = LatticeStateQuery.MapShardsByIndex(_shards);
        return mapped.Length == 0 ? 0 : mapped[^1].LiveKeys;
    }

    /// <summary>
    /// Contrast: map into an exact-width array but always sort it with
    /// <c>Array.Sort</c> and a <c>Comparison</c> delegate, i.e. take the
    /// allocation trim without the ordered fast path. It separates the array
    /// shape from the fast path, so neither is credited with the other's saving,
    /// and it shows what an unconditional comparison sort costs on an input that
    /// is already ordered.
    /// </summary>
    [Benchmark(Description = "Shard summaries: exact-width map + unconditional sort (contrast)")]
    public long Shards_Contrast_AlwaysSort()
    {
        var shards = _shards;
        var mapped = new ShardStateSummary[shards.Length];
        for (var i = 0; i < shards.Length; i++)
        {
            var shard = shards[i];
            mapped[i] = new ShardStateSummary
            {
                ShardIndex = shard.ShardIndex,
                Depth = shard.Depth,
                RootIsLeaf = shard.RootIsLeaf,
                LiveKeys = shard.LiveKeys,
                Tombstones = shard.Tombstones,
                OpsPerSecond = shard.OpsPerSecond,
                SplitInProgress = shard.SplitInProgress,
            };
        }

        Array.Sort(mapped, static (a, b) => a.ShardIndex.CompareTo(b.ShardIndex));
        return mapped.Length == 0 ? 0 : mapped[^1].LiveKeys;
    }
}
