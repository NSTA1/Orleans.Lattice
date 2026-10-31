using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Models view sample dispatch fusion, scalar runtime-view lookup, and tree-stat
/// read overlap. Dispatches are simulated, not real Orleans throughput or message
/// allocation evidence. Registry allocations model the existing array snapshot,
/// excluding serialization. Run with --suite adminsampling.
/// </summary>
[MemoryDiagnoser]
public class AdminSamplingBenchmarks
{
    /// <summary>The sampling path being modelled.</summary>
    public enum Site
    {
        /// <summary>Lag, active generation and count: three dispatches become two.</summary>
        ViewSample,
        /// <summary>One dictionary lookup replaces a 512-registration array and scan.</summary>
        RuntimeLookup,
        /// <summary>Two independent reads overlap; dispatch count remains two.</summary>
        TreeStats,
    }

    /// <summary>The caller path.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private sealed record Registration(string ViewName, bool IsAggregation);

    private readonly Dictionary<string, Registration> _registrations = new(StringComparer.Ordinal);
    private int _calls;

    /// <summary>Validate equal answers and the exact dispatch budget.</summary>
    [GlobalSetup]
    public void Setup()
    {
        for (var i = 0; i < 512; i++)
            _registrations.Add($"view-{i}", new Registration($"view-{i}", i % 2 == 0));
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedBefore = Path == Site.ViewSample ? 3 : Path == Site.RuntimeLookup ? 1 : 2;
        var expectedAfter = Path == Site.RuntimeLookup ? 1 : 2;
        if (before != after || beforeCalls != expectedBefore || _calls != expectedAfter)
            throw new InvalidOperationException("Sampling answer or dispatch budget differs.");
    }

    /// <summary>The previous caller.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        await DispatchAsync();
        if (Path == Site.RuntimeLookup)
        {
            var snapshot = _registrations.Values.ToArray();
            foreach (var registration in snapshot)
                if (registration.ViewName == "view-510")
                    return registration.IsAggregation ? 1 : 0;
            return -1;
        }
        await DispatchAsync();
        if (Path == Site.ViewSample)
            await DispatchAsync();
        return 42;
    }

    /// <summary>The fused, scalar or overlapped caller.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        if (Path == Site.TreeStats)
            await Task.WhenAll(DispatchAsync(), DispatchAsync());
        else
        {
            await DispatchAsync();
            if (Path == Site.RuntimeLookup)
                return _registrations.TryGetValue("view-510", out var registration)
                    ? registration.IsAggregation ? 1 : 0
                    : -1;
            await DispatchAsync();
        }
        return 42;
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
