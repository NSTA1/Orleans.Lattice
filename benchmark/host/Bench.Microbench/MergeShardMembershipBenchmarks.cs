using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures the exact old membership loop against the production merge helper,
/// without transport or mock-framework costs. Run with --suite shardcontrol.
/// </summary>
[MemoryDiagnoser]
public class MergeShardMembershipBenchmarks
{
    /// <summary>The number of routed source shards in a completed generation.</summary>
    [Params(16, 256, 4096)]
    public int Shards { get; set; }

    private int[] _recorded = [];
    private IReadOnlyList<int> _current = [];

    /// <summary>Build a sparse, sorted routing map and validate equivalence.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _recorded = Enumerable.Range(0, Shards).Select(index => index * 3).Reverse().ToArray();
        _current = _recorded.Reverse().ToArray();
        if (Before() || After())
            throw new InvalidOperationException("An unchanged generation contains no retired shards.");
    }

    /// <summary>The previous linear membership test for every recorded shard.</summary>
    [Benchmark(Baseline = true)]
    public bool Before()
    {
        for (var i = 0; i < _recorded.Length; i++)
            if (!_current.Contains(_recorded[i])) return true;
        return false;
    }

    /// <summary>The production binary-search membership test.</summary>
    [Benchmark]
    public bool After() => TreeMergeGrain.HasRetiredSourceShard(_recorded, 0, _current);
}
