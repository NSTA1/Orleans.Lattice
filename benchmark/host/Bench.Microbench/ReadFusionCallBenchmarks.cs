using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Models the dispatch removed by WAL activation metadata, observer resume-floor
/// and whole-tree structure reads. This is not an Orleans throughput benchmark:
/// provider I/O and topology traversal are unchanged and excluded in both lanes.
/// Delay1Ms ratios model serial hops; absolute times depend on OS timer resolution.
/// Run with --suite readfusion.
/// </summary>
[MemoryDiagnoser]
public class ReadFusionCallBenchmarks
{
    /// <summary>The independently optimized read paths.</summary>
    public enum Site
    {
        /// <summary>Fresh provenance and a cold partition pin share one entry read.</summary>
        WalActivation,
        /// <summary>Next sequence and live count are read inside one grain call.</summary>
        ObserverResume,
        /// <summary>Forced routing supplies both physical identity and shard map.</summary>
        TreeStructure,
    }

    /// <summary>The read path under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport model.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Check equivalent results and exact dispatch counts before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != 2 || _calls != 1)
            throw new InvalidOperationException("Read fusion changed the result or dispatch budget.");
    }

    /// <summary>The two separate grain calls used before fusion.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        var first = await DispatchAsync();
        var second = await DispatchAsync();
        return Path switch
        {
            Site.WalActivation => first.Identity.Length + second.Partitions,
            Site.ObserverResume => first.Next - second.Live,
            _ => first.Identity.Length + second.Shards,
        };
    }

    /// <summary>The combined call; backend work is deliberately not priced here.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        var read = await DispatchAsync();
        return Path switch
        {
            Site.WalActivation => read.Identity.Length + read.Partitions,
            Site.ObserverResume => read.Next - read.Live,
            _ => read.Identity.Length + read.Shards,
        };
    }

    private async Task<(string Identity, int Partitions, long Next, long Live, int Shards)> DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
        return ("logical-tree", 4, 100, 25, 8);
    }
}
