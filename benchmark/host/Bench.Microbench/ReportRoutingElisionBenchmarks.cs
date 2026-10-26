using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-cost model for report registry reuse and the first split sweep probe.
/// Report construction uses production options/routing; transport and the split
/// head read are modeled, not measured Orleans silo throughput.
/// </summary>
[MemoryDiagnoser]
public class ReportRoutingElisionBenchmarks
{
    private readonly TreeRegistryEntry _entry = new()
    {
        MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4,
        ShardMap = ShardMap.CreateDefault(16, 4),
    };
    private LatticeOptionsResolver _resolver = null!;
    private IGrainFactory _factory = null!;
    private int _dispatches;

    /// <summary>The three changed call sites.</summary>
    [ParamsAllValues]
    public CallSite Site { get; set; }

    /// <summary>Identical simulated transport on both sides.</summary>
    [ParamsAllValues]
    public RegistryProjectionElisionBenchmarks.HopModel Hop { get; set; }

    /// <summary>Rejects divergent results or unexpected dispatch counts.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _factory = Substitute.For<IGrainFactory>();
        _resolver = new LatticeOptionsResolver(_factory, new FixedOptions());
        _dispatches = 0;
        var before = Before().GetAwaiter().GetResult();
        var beforeCalls = _dispatches;
        _dispatches = 0;
        var after = After().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (Site == CallSite.PreparedSweep ? 2 : 3) || _dispatches != 1)
            throw new InvalidOperationException("Result or dispatch-count equivalence failed.");
    }

    /// <summary>Previous complete-pin report path or double first-head probe.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> Before()
    {
        await DispatchAsync();
        if (Site == CallSite.PreparedSweep)
        {
            await DispatchAsync();
            return 1;
        }
        var options = _resolver.Resolve("report-bench", _entry);
        await DispatchAsync(); // ILattice stateless worker.
        await DispatchAsync(); // Its fresh registry read.
        return options.MaxLeafKeys + _entry.ShardMap!.VirtualShardCount;
    }

    /// <summary>One fresh row with production routing, or one first-head probe.</summary>
    [Benchmark]
    public async Task<int> After()
    {
        await DispatchAsync();
        if (Site == CallSite.PreparedSweep) return 1;
        var options = _resolver.Resolve("report-bench", _entry);
        var routing = await ReportRouting.ResolveAsync(_factory, "report-bench", _entry, CancellationToken.None);
        return options.MaxLeafKeys + routing.Map.VirtualShardCount;
    }

    private async Task DispatchAsync()
    {
        _dispatches++;
        if (Hop == RegistryProjectionElisionBenchmarks.HopModel.Delay1Ms)
            await Task.Delay(1);
        else
            await Task.Yield();
    }

    /// <summary>Independent optimized consumers.</summary>
    public enum CallSite
    {
        /// <summary>Diagnostic report cache miss.</summary>
        Diagnostics,
        /// <summary>Storage report cache miss, excluding unchanged WAL reads.</summary>
        StorageUsage,
        /// <summary>First nonempty prepared sweep; retries are unchanged.</summary>
        PreparedSweep,
    }

    private sealed class FixedOptions : IOptionsMonitor<LatticeOptions>
    {
        public LatticeOptions CurrentValue { get; } = new();
        public LatticeOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<LatticeOptions, string?> listener) => null;
    }
}
