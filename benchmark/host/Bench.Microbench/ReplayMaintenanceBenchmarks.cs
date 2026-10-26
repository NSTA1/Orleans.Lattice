using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-shape evidence for replay maintenance. Extends the reseedsampling
/// model; Yield and Delay1Ms are not measurements of live Orleans throughput.
/// Run with --suite replaymaintenance.
/// </summary>
[MemoryDiagnoser]
public class ReplayMaintenanceBenchmarks
{
    /// <summary>The optimized production site.</summary>
    public enum Site
    {
        /// <summary>Forty prepares in one registry shard, reduced to one bounded batch.</summary>
        PrepareBatch,
        /// <summary>No participants and no recorded terminal, reduced from two reads to one.</summary>
        PurgeProbe,
        /// <summary>Forty independent registry stamp reads, overlapped at width 32.</summary>
        StampLookups,
    }

    /// <summary>The site measured.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness hop model.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;
    private const int Count = 40;

    /// <summary>Verify result equivalence and the exact dispatch budgets.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (Path == Site.PurgeProbe ? 2 : Count)
            || _calls != (Path == Site.StampLookups ? Count : 1))
            throw new InvalidOperationException("Replay maintenance result or call budget differs.");
    }

    /// <summary>The previous serial dispatch shape.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        if (Path == Site.PurgeProbe)
        {
            await DispatchAsync(0);
            return await DispatchAsync(1);
        }
        var sum = 0;
        for (var i = 0; i < Count; i++) sum += await DispatchAsync(1);
        return sum;
    }

    /// <summary>The bounded batch, fused predicate, or settled fan-out.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        if (Path == Site.PurgeProbe) return await DispatchAsync(1);
        if (Path == Site.PrepareBatch) return await DispatchAsync(Count);
        var sum = 0;
        await BoundedFanOut.ForEachAsync(Enumerable.Range(0, Count).ToArray(),
            BoundedFanOut.DefaultWidth, async _ =>
            {
                var value = await DispatchAsync(1);
                Interlocked.Add(ref sum, value);
            });
        return sum;
    }

    private async Task<int> DispatchAsync(int result)
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
        return result;
    }
}
