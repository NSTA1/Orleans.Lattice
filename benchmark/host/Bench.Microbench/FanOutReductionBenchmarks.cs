using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Fan-out-reduction workload: measures the per-operation cost of the three read
/// sites the change batched, comparing the pre-change sequential-read shape
/// against the shipped single batched multi-get. Each site collapses N sequential
/// awaited grain reads into one, so the baseline arm pays N scheduling latencies
/// and allocates N awaited <see cref="System.Threading.Tasks.Task"/> state
/// machines where the batched arm pays one.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it compares.</b> The <c>*_Baseline</c> arms reproduce the production
/// loops verbatim (see <see cref="FanOutShapes"/> for the exact source sites); the
/// <c>*_Batched</c> arms issue the single <see cref="FanOutReadSurface.GetManyAsync"/>
/// the change replaced them with. Both run over the same counting read surface, so
/// the column delta is exactly the call-shape change. The exact round-trip
/// reduction is reported separately and deterministically by
/// <see cref="FanOutRoundTripReport"/>, because hop count is exact and needs no
/// statistics.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=fanout</c> (or <c>--suite fanout</c>); see
/// <c>Program.cs</c>. No Orleans silo is involved.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class FanOutReductionBenchmarks
{
    private static readonly byte[] Row = [1];

    /// <summary>Sibling tags probed per candidate in the AND-query arms.</summary>
    [Params(4, 8)]
    public int Tags { get; set; }

    /// <summary>Keys per atomic step / shard slots per view materialisation.</summary>
    [Params(16, 64)]
    public int Width { get; set; }

    private const int AndCandidates = 100;

    private FanOutReadSurface _andSurface = null!;
    private List<IReadOnlyList<string>> _andProbeKeys = null!;
    private FanOutReadSurface _widthSurface = null!;
    private List<string> _keys = null!;

    /// <summary>Builds the seeded surfaces and the pre-partitioned probe inputs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var andSeed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        _andProbeKeys = new List<IReadOnlyList<string>>(AndCandidates);
        for (var c = 0; c < AndCandidates; c++)
        {
            var probe = new List<string>(Tags - 1);
            for (var t = 1; t < Tags; t++)
            {
                var key = string.Create(CultureInfo.InvariantCulture, $"tag{t}\0tree\0k{c}");
                andSeed[key] = Row;
                probe.Add(key);
            }

            _andProbeKeys.Add(probe);
        }

        _andSurface = new FanOutReadSurface(andSeed);

        var widthSeed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        _keys = new List<string>(Width);
        for (var i = 0; i < Width; i++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"k{i}");
            widthSeed[key] = Row;
            _keys.Add(key);
        }

        _widthSurface = new FanOutReadSurface(widthSeed);
    }

    /// <summary>Baseline: (T-1) sequential membership reads per candidate.</summary>
    [Benchmark(Description = "Tag-index AND: per-tag sequential probe (baseline)")]
    public Task<int> TagIndexAnd_Baseline() => FanOutShapes.TagIndexAndBaselineAsync(_andSurface, _andProbeKeys);

    /// <summary>Shipped: one batched membership multi-get per candidate.</summary>
    [Benchmark(Description = "Tag-index AND: batched probe (shipped)")]
    public Task<int> TagIndexAnd_Batched() => FanOutShapes.TagIndexAndBatchedAsync(_andSurface, _andProbeKeys);

    /// <summary>Baseline: one sequential read per written key.</summary>
    [Benchmark(Description = "Atomic pre-image: per-key sequential read (baseline)")]
    public Task<int> AtomicPreImage_Baseline() => FanOutShapes.AtomicPreImageBaselineAsync(_widthSurface, _keys);

    /// <summary>Shipped: one batched read for the whole step.</summary>
    [Benchmark(Description = "Atomic pre-image: batched read (shipped)")]
    public Task<int> AtomicPreImage_Batched() => FanOutShapes.AtomicPreImageBatchedAsync(_widthSurface, _keys);

    /// <summary>Baseline: one sequential read per inverse shard slot.</summary>
    [Benchmark(Description = "View inverse: per-slot sequential read (baseline)")]
    public Task<int> ViewInverse_Baseline() => FanOutShapes.ViewInverseBaselineAsync(_widthSurface, _keys);

    /// <summary>Shipped: one batched read across all inverse shard slots.</summary>
    [Benchmark(Description = "View inverse: batched read (shipped)")]
    public Task<int> ViewInverse_Batched() => FanOutShapes.ViewInverseBatchedAsync(_widthSurface, _keys);
}
