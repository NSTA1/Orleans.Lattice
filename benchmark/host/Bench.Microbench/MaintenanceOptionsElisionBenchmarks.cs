using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures one awaited registry dispatch plus full options construction against
/// the production configuration-only maintenance paths. This is not a silo
/// throughput benchmark; the delay model includes the platform timer floor.
/// </summary>
[MemoryDiagnoser]
public class MaintenanceOptionsElisionBenchmarks
{
    private const string TreeId = "maintenance-bench";
    private LatticeOptionsResolver _resolver = null!;
    private readonly TreeRegistryEntry _entry = new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 4,
        WalPartitions = 4,
    };

    /// <summary>The configuration-only consumer being measured.</summary>
    [ParamsAllValues]
    public MaintenanceLane Lane { get; set; }

    /// <summary>The same dispatch models as the existing registry benchmark.</summary>
    [ParamsAllValues]
    public RegistryProjectionElisionBenchmarks.HopModel Hop { get; set; }

    /// <summary>Warms option construction and asserts consumer equivalence.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _resolver = new LatticeOptionsResolver(Substitute.For<IGrainFactory>(), new FixedOptions());
        var full = _resolver.Resolve(TreeId, _entry);
        var configured = _resolver.GetConfiguredOptions(TreeId);
        if (full.BackgroundDrainLeavesPerPass != configured.BackgroundDrainLeavesPerPass
            || full.BackgroundDrainMaxDuration != configured.BackgroundDrainMaxDuration)
            throw new InvalidOperationException("Maintenance budget bounds must agree before measurement.");
        if (Consume(full) != ConsumeConfigured())
            throw new InvalidOperationException("Maintenance consumers must agree before measurement.");
    }

    /// <summary>
    /// Prices the old complete-pin path conservatively: one dispatch followed by
    /// actual production full options construction, without mock call recording.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task<long> RegistryOptions()
    {
        if (Hop == RegistryProjectionElisionBenchmarks.HopModel.Delay1Ms)
            await Task.Delay(1);
        else
            await Task.Yield();
        return Consume(_resolver.Resolve(TreeId, _entry));
    }

    /// <summary>Invokes the production no-RPC budget or scheduling consumer.</summary>
    [Benchmark]
    public long ConfiguredOptions() => ConsumeConfigured();

    private long ConsumeConfigured()
    {
        if (Lane == MaintenanceLane.CompactionSchedule)
        {
            var schedule = _resolver.GetCompactionSchedule(TreeId);
            return schedule.TickInterval.Ticks + schedule.LeafBatchSize;
        }
        return Consume(_resolver.GetConfiguredOptions(TreeId));
    }

    private long Consume(LatticeOptions options)
    {
        if (Lane == MaintenanceLane.CompactionSchedule)
            return options.CompactionShardTickInterval.Ticks + options.CompactionLeafBatchSize;
        var budget = Lane switch
        {
            MaintenanceLane.SnapshotBudget => LeafWalkBudget.ForBackgroundDrain(options),
            MaintenanceLane.OrphanBudget => LeafWalkBudget.ForOrphanedLeafPass(options),
            _ => throw new InvalidOperationException("Unknown maintenance lane."),
        };
        budget.RecordLeafVisited();
        return budget.ShouldYield() ? 0 : budget.LeavesVisited;
    }

    /// <summary>The three independently elided maintenance reads.</summary>
    public enum MaintenanceLane
    {
        /// <summary>An online snapshot's leaf and time budget.</summary>
        SnapshotBudget,
        /// <summary>An orphan pass's time-only budget.</summary>
        OrphanBudget,
        /// <summary>A compaction pass's clamped interval and leaf cap.</summary>
        CompactionSchedule,
    }

    private sealed class FixedOptions : IOptionsMonitor<LatticeOptions>
    {
        public LatticeOptions CurrentValue { get; } = new();
        public LatticeOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<LatticeOptions, string?> listener) => null;
    }
}
