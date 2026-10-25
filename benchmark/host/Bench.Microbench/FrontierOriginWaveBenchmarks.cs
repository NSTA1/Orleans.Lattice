using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-only model of ReplicationTreeFrontierGrain's three origin waves.
/// Like PartitionWaveBenchmarks, compares the previous serial awaits with the
/// shipped bounded helper. The total calls do not change. Task.Delay(1) models
/// a remote hop; Windows can round it up, so compare ratios, not absolute times.
/// Does not measure end-to-end Orleans throughput or storage durability cost.
/// Run with --suite frontierwaves.
/// </summary>
[MemoryDiagnoser]
public class FrontierOriginWaveBenchmarks
{
    /// <summary>The three independent protocol phases.</summary>
    public enum Site
    {
        /// <summary>Zero every origin cap before resetting identities.</summary>
        ReplacementCaps,
        /// <summary>Install every held set before raising any export cap.</summary>
        ExportHeld,
        /// <summary>Raise every capped origin before persisting the pin.</summary>
        ExportCaps,
    }

    /// <summary>The phase whose dispatch shape is measured.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>Small deployment and a ragged wave beyond the production bound.</summary>
    [Params(4, 40)]
    public int OriginCount { get; set; }

    private int[] _origins = null!;
    private int _calls;
    private long _checksum;

    /// <summary>Validate equal effects and unchanged call counts before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _origins = Enumerable.Range(0, OriginCount).ToArray();
        var before = SerialAsync().GetAwaiter().GetResult();
        if (_calls != OriginCount)
            throw new InvalidOperationException("Serial call count differs from the origin count.");
        var after = BoundedAsync().GetAwaiter().GetResult();
        if (_calls != OriginCount || before != after)
            throw new InvalidOperationException("Bounded wave changed the effects or call count.");
    }

    /// <summary>The former serial per-origin phase.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> SerialAsync()
    {
        _calls = 0;
        _checksum = 0;
        foreach (var origin in _origins)
            await DispatchAsync(origin);
        return _checksum;
    }

    /// <summary>The production helper; the phase barrier stays after its await.</summary>
    [Benchmark]
    public async Task<long> BoundedAsync()
    {
        _calls = 0;
        _checksum = 0;
        await BoundedFanOut.ForEachAsync(_origins, BoundedFanOut.DefaultWidth, DispatchAsync);
        return _checksum;
    }

    private async Task DispatchAsync(int origin)
    {
        Interlocked.Increment(ref _calls);
        await Task.Delay(1);
        Interlocked.Add(ref _checksum, Path switch
        {
            Site.ReplacementCaps => origin + 1L,
            Site.ExportHeld => (origin + 1L) * 12,
            Site.ExportCaps => (origin + 1L) * 30,
            _ => throw new InvalidOperationException("Unknown frontier phase."),
        });
    }
}
