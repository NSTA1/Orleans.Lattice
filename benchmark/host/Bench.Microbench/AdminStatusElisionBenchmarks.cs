using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-only model of the tree-admin status-read elisions. Each site keeps the
/// same coordinator reads and answer; only the transport dispatches disappear:
/// resize status drops from five hops (gate, idle, undo, progress via the tree
/// grain) to one fused coordinator read, reshard status from three to one, and
/// view status from two maintainer reads to one. Not real Orleans throughput or
/// message allocations. Run with --suite adminstatus.
/// </summary>
[MemoryDiagnoser]
public class AdminStatusElisionBenchmarks
{
    /// <summary>The tree-admin status read being modelled.</summary>
    public enum Site
    {
        /// <summary>Resize status: tree-grain gate plus idle, undo and progress reads, versus one fused read.</summary>
        ResizeStatus,
        /// <summary>Reshard status: tree-grain gate plus idle and progress reads, versus progress alone.</summary>
        ReshardStatus,
        /// <summary>View status: lag and active-tree reads, versus one fused read.</summary>
        ViewStatus,
    }

    /// <summary>The production caller model.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Prove equal answers and exact dispatch counts before measuring.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != BeforeHops || _calls != 1)
            throw new InvalidOperationException("Status answer or dispatch count differs.");
    }

    /// <summary>The previous sequence of separate grain calls.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        long answer = 0;
        for (var i = 0; i < BeforeHops; i++)
        {
            await DispatchAsync();
            answer += Read(i);
        }
        return answer;
    }

    /// <summary>One fused coordinator read returning the same projection.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        await DispatchAsync();
        long answer = 0;
        for (var i = 0; i < BeforeHops; i++)
            answer += Read(i);
        return answer;
    }

    private int BeforeHops => Path switch
    {
        Site.ResizeStatus => 5,
        Site.ReshardStatus => 3,
        _ => 2,
    };

    private static long Read(int field) => field * 31L + 7;

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
