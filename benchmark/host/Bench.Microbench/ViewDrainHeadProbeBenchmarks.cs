using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-model evidence for the WAL head probes one view-maintainer drain tick
/// pays across the default eight WAL partitions. The subscriber's fall-off guard
/// probed each partition's readable head serially, and the maintainer then
/// re-probed every head (in one bounded wave) to record its lag.
/// <list type="bullet">
/// <item><see cref="Site.IdleTick"/> - the fixed-cadence tick that reads and applies
/// nothing: 8 serial probes + an 8-probe lag wave (16 calls) become one 8-probe
/// wave whose heads the lag folds (8 calls).</item>
/// <item><see cref="Site.BusyTick"/> - a tick that reads two partitions: the guard
/// probes overlap in one wave, the two page reads stay serial, and the lag still
/// re-probes so arrivals during the apply are counted (18 calls either way).</item>
/// </list>
/// Yield measures scheduling overhead; Delay1Ms models latency, not real Orleans
/// throughput. Run with --suite viewdrainheads.
/// </summary>
[MemoryDiagnoser]
public class ViewDrainHeadProbeBenchmarks
{
    /// <summary>The drain tick shape.</summary>
    public enum Site
    {
        /// <summary>No backlog: probes are the whole tick.</summary>
        IdleTick,
        /// <summary>Two partitions with backlog.</summary>
        BusyTick,
    }

    private const int Partitions = 8;
    private const int BusyPartitions = 2;

    /// <summary>The tick under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

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
        _calls = 0;
        _peak = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var reads = Path == Site.BusyTick ? BusyPartitions : 0;
        var expectedBefore = (2 * Partitions) + reads;
        var expectedAfter = Path == Site.IdleTick ? Partitions : expectedBefore;
        if (before != after || beforeCalls != expectedBefore || _calls != expectedAfter
            || _active != 0 || _peak < 2 || _peak > Partitions)
            throw new InvalidOperationException("View drain head-probe result or dispatch budget differs.");
    }

    /// <summary>Serial guard probes, page reads, then a fresh lag wave.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        var heads = new long[Partitions];
        for (var p = 0; p < Partitions; p++) heads[p] = await ProbeAsync(p);
        await ReadBacklogAsync();
        var lag = await BoundedFanOut.RunAsync(Partitions, BoundedFanOut.DefaultWidth, ProbeAsync);
        return lag.Sum() + heads.Sum();
    }

    /// <summary>One overlapped guard wave; an idle tick folds lag from it.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        var heads = await BoundedFanOut.RunAsync(Partitions, BoundedFanOut.DefaultWidth, ProbeAsync);
        await ReadBacklogAsync();
        var lag = Path == Site.IdleTick
            ? heads
            : await BoundedFanOut.RunAsync(Partitions, BoundedFanOut.DefaultWidth, ProbeAsync);
        return lag.Sum() + heads.Sum();
    }

    private async Task ReadBacklogAsync()
    {
        if (Path != Site.BusyTick) return;
        for (var p = 0; p < BusyPartitions; p++) await ProbeAsync(p);
    }

    private async Task<long> ProbeAsync(int partition)
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

        return partition;
    }
}
