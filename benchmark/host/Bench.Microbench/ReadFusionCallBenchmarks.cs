using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Models the dispatch removed by WAL activation metadata, observer resume-floor
/// and whole-tree structure reads. This is not an Orleans throughput benchmark:
/// provider I/O and topology traversal are unchanged and excluded in both lanes.
/// Delay1Ms ratios model serial hops; absolute times depend on OS timer resolution.
/// Run with --suite readfusion.
/// The --suite ownershipbarriers subset models fresh structural ownership checks,
/// re-add index reads and matching-origin barrier abandonment. It excludes the
/// unchanged alias survey, index withdrawals and persistence. Allocations are
/// those of the dispatch model, not production row/status serialization.
/// The --suite decommissionmerkle subset measures peer marker and Merkle node
/// 2-to-1 calls and one-owner pending verdict reads (32-to-1, below the 128 cap).
/// It excludes unchanged marker persistence and registry retention/delegation work.
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
        /// <summary>Missing structural tree: exists, derivation and resolution become one row read.</summary>
        AppOwnership,
        /// <summary>Re-add reads live and tombstoned keys in one index call.</summary>
        BarrierIndex,
        /// <summary>Matching-origin status and abandonment become one serialized call.</summary>
        BarrierAbandon,
        /// <summary>Peer marker observation and durable mark share one dispatch.</summary>
        PeerDecommission,
        /// <summary>Merkle internal-node digest and routing share one dispatch.</summary>
        MerkleNode,
        /// <summary>32 pending transactions on one registry share one status call.</summary>
        DecommissionVerdicts,
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
        var expectedCalls = Path == Site.DecommissionVerdicts ? 32 : Path == Site.AppOwnership ? 3 : 2;
        if (before != after || beforeCalls != expectedCalls || _calls != 1)
            throw new InvalidOperationException("Read fusion changed the result or dispatch budget.");
    }

    /// <summary>The separate grain calls used before fusion.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        if (Path == Site.DecommissionVerdicts)
        {
            long total = 0;
            for (var i = 0; i < 32; i++) total += (await DispatchAsync()).Live;
            return total;
        }
        var first = await DispatchAsync();
        var second = await DispatchAsync();
        if (Path == Site.AppOwnership)
        {
            var third = await DispatchAsync();
            return first.Identity.Length + second.Partitions + third.Shards;
        }
        return Path switch
        {
            Site.WalActivation => first.Identity.Length + second.Partitions,
            Site.ObserverResume => first.Next - second.Live,
            Site.BarrierIndex => first.Partitions + second.Shards,
            Site.BarrierAbandon => first.Identity.Length + second.Live,
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
            Site.AppOwnership => read.Identity.Length + read.Partitions + read.Shards,
            Site.BarrierIndex => read.Partitions + read.Shards,
            Site.BarrierAbandon => read.Identity.Length + read.Live,
            Site.DecommissionVerdicts => 32 * read.Live,
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
