using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Warm-activation dispatch model for partition heads, cache refresh and migration freeze.
/// Yield and Delay1Ms model transport scheduling/latency, not Orleans throughput
/// or product allocations. Cold activation still resolves options once.
/// Run with --suite shardcontrol.
/// </summary>
[MemoryDiagnoser]
public class ShardControlCallBenchmarks
{
    /// <summary>The independently optimized entry points.</summary>
    public enum Site
    {
        /// <summary>Four head reads: five calls become four.</summary>
        SnapshotHead,
        /// <summary>Four head reads and one leaf page: six calls become five.</summary>
        MaterialiserLag,
        /// <summary>Pending-key snapshot and cursor delta: two calls become one.</summary>
        CacheRefresh,
        /// <summary>Source leaf seal and Reject persistence: two calls become one.</summary>
        SplitFreeze,
        /// <summary>Donor leaf seal and Reject persistence: two calls become one.</summary>
        ConsolidationFreeze,
    }

    /// <summary>The operation under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Validate output equality and exact dispatch counts before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedAfter = Path switch
        {
            Site.SnapshotHead => 4,
            Site.MaterialiserLag => 5,
            _ => 1,
        };
        if (before != after || beforeCalls != expectedAfter + 1 || _calls != expectedAfter)
            throw new InvalidOperationException("Shard control result or dispatch budget differs.");
    }

    /// <summary>The previous separate registry/pending/seal dispatch followed by its remaining work.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        await DispatchAsync();
        return await CaptureAsync();
    }

    /// <summary>One fewer dispatch; the heads or constituent observations/writes remain ordered.</summary>
    [Benchmark]
    public Task<long> AfterAsync() => CaptureAsync();

    private async Task<long> CaptureAsync()
    {
        if (Path is Site.CacheRefresh or Site.SplitFreeze or Site.ConsolidationFreeze)
        {
            await DispatchAsync();
            return 42;
        }
        var heads = await BoundedFanOut.RunAsync(4, BoundedFanOut.DefaultWidth, async partition =>
        {
            await DispatchAsync();
            return 42L + partition;
        });
        if (Path == Site.MaterialiserLag)
        {
            await DispatchAsync();
            return heads.Sum(head => head - 7);
        }
        return heads.Sum();
    }

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
