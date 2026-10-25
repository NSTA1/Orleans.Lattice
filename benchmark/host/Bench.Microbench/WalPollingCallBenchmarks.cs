using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-model evidence for idle subscriber reads, retention-tail reads and
/// independent view-alias notifications. Uses the existing leaf-chain hop model:
/// Yield measures scheduling overhead; Delay1Ms models latency, not real Orleans
/// throughput. Provider I/O inside the combined tail call is deliberately unchanged.
/// Run with --suite walpolling.
/// </summary>
[MemoryDiagnoser]
public class WalPollingCallBenchmarks
{
    /// <summary>The three optimized call sites.</summary>
    public enum Site
    {
        /// <summary>Four idle partitions: eight calls become four.</summary>
        IdleDrain,
        /// <summary>Trusted retention watermark: two calls become one.</summary>
        RetentionTail,
        /// <summary>Thirty-two independent notifications: same calls, one bounded wave.</summary>
        ViewAlias,
    }

    /// <summary>The call site under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private static readonly int[] Views = Enumerable.Range(0, BoundedFanOut.DefaultWidth).ToArray();
    private int _calls;
    private int _active;
    private int _peak;

    /// <summary>Check result equality and call budgets before collecting measurements.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        _peak = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        var beforePeak = _peak;
        _calls = 0;
        _peak = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedBefore = Path switch { Site.IdleDrain => 8, Site.RetentionTail => 2, _ => Views.Length };
        var expectedAfter = Path switch { Site.IdleDrain => 4, Site.RetentionTail => 1, _ => Views.Length };
        var maxPeak = Path == Site.ViewAlias ? BoundedFanOut.DefaultWidth : 1;
        if (before != after || beforeCalls != expectedBefore || _calls != expectedAfter
            || _active != 0 || beforePeak != 1 || _peak < 1 || _peak > maxPeak)
            throw new InvalidOperationException("WAL polling result or dispatch budget differs.");
    }

    /// <summary>The prior sequential grain-call shape.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        switch (Path)
        {
            case Site.IdleDrain:
                for (var p = 0; p < 4; p++) await DispatchAsync(); // Head probes.
                for (var p = 0; p < 4; p++) await DispatchAsync(); // Empty page reads.
                return 0;
            case Site.RetentionTail:
                await DispatchAsync(); // Watermark.
                await DispatchAsync(); // Allocator.
                return 42;
            default:
                foreach (var view in Views) await DispatchAsync();
                return Views.Length;
        }
    }

    /// <summary>The optimized dispatch shape, without changing backend work.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        switch (Path)
        {
            case Site.IdleDrain:
                var hasBacklog = new bool[4];
                for (var p = 0; p < 4; p++) await DispatchAsync();
                for (var p = 0; p < hasBacklog.Length; p++)
                    if (hasBacklog[p]) await DispatchAsync();
                return 0;
            case Site.RetentionTail:
                await DispatchAsync();
                return 42;
            default:
                await BoundedFanOut.ForEachAsync(Views, BoundedFanOut.DefaultWidth, _ => DispatchAsync());
                return Views.Length;
        }
    }

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        var active = Interlocked.Increment(ref _active);
        int observed;
        do { observed = Volatile.Read(ref _peak); }
        while (active > observed && Interlocked.CompareExchange(ref _peak, active, observed) != observed);
        try
        {
            if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
                await Task.Yield();
            else
                await Task.Delay(1);
        }
        finally { Interlocked.Decrement(ref _active); }
    }
}
