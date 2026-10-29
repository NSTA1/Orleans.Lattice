using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Bootstrap transport-call models, not end-to-end throughput. Payload work is
/// held constant; real leaf semantics and coordinator wiring have regression
/// coverage. ForeignScan retains the old overlapping dispatch, not a fictional
/// serial baseline. Run with --suite bootstrapcalls.
/// </summary>
[MemoryDiagnoser]
public class BootstrapCallReductionBenchmarks
{
    /// <summary>The independently measured call reduction.</summary>
    public enum Site
    {
        /// <summary>Delta followed by live entries and the sibling pointer.</summary>
        PreCapture,
        /// <summary>Overlapped live and pending reads become one leaf call.</summary>
        ForeignScan,
        /// <summary>Four stale saga discards become one call and persistence per leaf.</summary>
        Discard,
    }

    /// <summary>The bootstrap operation.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The chain length.</summary>
    [Params(4)]
    public int Leaves { get; set; }

    /// <summary>How simulated transport dispatch completes.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Pin output equivalence and exact call counts before measuring.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var width = Path == Site.Discard ? 4 : 2;
        if (before != after || after != Leaves * width || beforeCalls != Leaves * width || _calls != Leaves)
            throw new InvalidOperationException("Bootstrap output or dispatch budget changed.");
    }

    /// <summary>The previous call pattern.</summary>
    [Benchmark(Baseline = true)]
    public Task<int> BeforeAsync() => WalkAsync(combined: false);

    /// <summary>The combined call pattern.</summary>
    [Benchmark]
    public Task<int> AfterAsync() => WalkAsync(combined: true);

    private async Task<int> WalkAsync(bool combined)
    {
        var total = 0;
        var width = Path == Site.Discard ? 4 : 2;
        for (var leaf = 0; leaf < Leaves; leaf++)
        {
            if (combined)
            {
                await DispatchAsync();
            }
            else if (Path == Site.ForeignScan)
            {
                await Task.WhenAll(DispatchAsync(), DispatchAsync());
            }
            else
            {
                for (var i = 0; i < width; i++) await DispatchAsync();
            }
            total += width;
        }
        return total;
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
