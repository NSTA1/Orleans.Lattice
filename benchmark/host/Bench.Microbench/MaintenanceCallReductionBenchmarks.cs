using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-model evidence for the maintenance call reductions. Yield models
/// scheduling; Delay1Ms includes the platform timer floor, not Orleans throughput.
/// Production regression tests separately pin the exact RPC budgets and ordering.
/// Run with --suite maintenancecalls.
/// </summary>
[MemoryDiagnoser]
public class MaintenanceCallReductionBenchmarks
{
    /// <summary>The optimized sites and the emptiness miss control.</summary>
    public enum Site
    {
        /// <summary>The first of 32 physical shards holds a live key.</summary>
        EmptinessHit,
        /// <summary>All 32 shards are empty; call count is unchanged.</summary>
        EmptinessMiss,
        /// <summary>Seven internal nodes with a three-node leftmost descent.</summary>
        PurgeDescent,
        /// <summary>32 chain leaves, each read then cleared.</summary>
        PurgeClear,
    }

    /// <summary>The maintenance call shape.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing benchmark harness's transport models.</summary>
    [ParamsAllValues]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Assert exact call budgets before collecting timings.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        BeforeAsync().GetAwaiter().GetResult();
        var before = _calls;
        _calls = 0;
        AfterAsync().GetAwaiter().GetResult();
        var expected = Path switch
        {
            Site.EmptinessHit => (32, 1),
            Site.EmptinessMiss => (32, 32),
            Site.PurgeDescent => (10, 7),
            _ => (64, 32),
        };
        if (before != expected.Item1 || _calls != expected.Item2)
            throw new InvalidOperationException("Maintenance RPC budget differs.");
    }

    /// <summary>The previous call shape.</summary>
    [Benchmark(Baseline = true)]
    public Task BeforeAsync() => RunAsync(before: true);

    /// <summary>The reduced call shape, including the miss latency tradeoff.</summary>
    [Benchmark]
    public Task AfterAsync() => RunAsync(before: false);

    private async Task RunAsync(bool before)
    {
        if (Path is Site.EmptinessHit or Site.EmptinessMiss)
        {
            var start = 0;
            if (!before)
            {
                await DispatchAsync();
                if (Path == Site.EmptinessHit) return;
                start = 1;
            }
            var tasks = new Task[32 - start];
            for (var i = 0; i < tasks.Length; i++) tasks[i] = DispatchAsync();
            await Task.WhenAll(tasks);
            return;
        }

        if (Path == Site.PurgeDescent)
        {
            if (before)
                for (var level = 0; level < 3; level++) await DispatchAsync();
            for (var width = 1; width <= 4; width *= 2)
            {
                var tasks = new Task[width];
                for (var i = 0; i < width; i++) tasks[i] = DispatchAsync();
                await Task.WhenAll(tasks);
            }
            return;
        }

        var count = before ? 64 : 32;
        for (var i = 0; i < count; i++) await DispatchAsync();
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
