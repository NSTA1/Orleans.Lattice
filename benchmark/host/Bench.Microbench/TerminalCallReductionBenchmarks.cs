using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three terminal-delivery dispatch reductions. This is a transport
/// model, not end-to-end Orleans throughput: WAL I/O, replay, instrumentation
/// and topology changes are excluded. The real call budgets and invalidation
/// fallbacks are pinned by ShardRootGrainTxTerminalTests.CallReduction.cs.
/// Run with --suite terminalcalls. Delay1Ms absolute times depend on the OS timer.
/// </summary>
[MemoryDiagnoser]
public class TerminalCallReductionBenchmarks
{
    /// <summary>Independently measured reductions, without double-counting.</summary>
    public enum Site
    {
        /// <summary>Reuse the initial chain at delivery; retain separate clock reads.</summary>
        ChainReuse,
        /// <summary>Fuse clocks into the first walk; retain the second chain walk.</summary>
        ClockFusion,
        /// <summary>A tracked saga without a WAL writer does not need clocks.</summary>
        NoWriter,
    }

    /// <summary>Optimization being isolated.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>Leaves on the shard.</summary>
    [Params(4)]
    public int Leaves { get; set; }

    /// <summary>Simulated dispatch completion.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Validate matching delivery and exact dispatch counts before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedBefore = (Path == Site.NoWriter ? 2 : 4) * Leaves;
        var expectedAfter = (Path == Site.NoWriter ? 1 : 3) * Leaves;
        if (before != after || before != Leaves || beforeCalls != expectedBefore || _calls != expectedAfter)
            throw new InvalidOperationException("Terminal delivery or dispatch budget changed.");
    }

    /// <summary>The original per-leaf call pattern.</summary>
    [Benchmark(Baseline = true)]
    public Task<int> BeforeAsync() => DeliverAsync(optimized: false);

    /// <summary>One reduction in isolation, holding the other two constant.</summary>
    [Benchmark]
    public Task<int> AfterAsync() => DeliverAsync(optimized: true);

    private async Task<int> DeliverAsync(bool optimized)
    {
        if (Path != Site.NoWriter)
            await WalkAsync(); // links, or fused clock/link pages

        if (!(optimized && Path is Site.ClockFusion or Site.NoWriter))
            await FanOutAsync(); // clock sampling

        if (Path != Site.NoWriter && !(optimized && Path == Site.ChainReuse))
            await WalkAsync(); // delivery's second chain discovery

        await FanOutAsync(); // terminal apply, unchanged
        return Leaves;
    }

    private async Task WalkAsync()
    {
        for (var i = 0; i < Leaves; i++) await DispatchAsync();
    }

    private async Task FanOutAsync()
    {
        var pending = new Task[Leaves];
        for (var i = 0; i < pending.Length; i++) pending[i] = DispatchAsync();
        await Task.WhenAll(pending);
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
