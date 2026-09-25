using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Drives the production GC on a blocked tree with more pins than its eight-id
/// diagnostic cap. Measures the residual scan required by the uncapped census.
/// Run with --suite blockedcensus; compare the same harness at the parent commit.
/// </summary>
[MemoryDiagnoser]
public class WalGcBlockedCensusBenchmarks
{
    private ServiceProvider _services = null!;
    private LatticeWalGc _gc = null!;

    /// <summary>Small and incident-sized blocked populations.</summary>
    [Params(64, 10000)]
    public int Consumers { get; set; }

    /// <summary>Builds cached pin responses without mocking allocations.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var pins = new Dictionary<string, HybridLogicalClock>(Consumers, StringComparer.Ordinal);
        for (var i = 0; i < Consumers; i++)
            pins.Add($"_lattice_materialiser_census-bench_leaf-{i}_0", HybridLogicalClock.Zero);
        var grain = new PinGrain(pins);
        var factory = new FakeGrainFactory();
        factory.RouteByString<IWalMaterialiserPinGrain>(_ => grain);
        _services = new ServiceCollection().AddSingleton<IGrainFactory>(factory)
            .AddSingleton<IWalStorageProvider>(new InMemoryWalStorageProvider()).BuildServiceProvider();
        _gc = new LatticeWalGc(_services, new InMemoryWalCursorRegistry(),
            new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions
            {
                WalPartitions = 1, WalMaterialiserPinShards = 1, WalDurabilityHoldCeilingBytes = 0,
            }));
        _gc.RunOnceAsync("census-bench").GetAwaiter().GetResult();
    }

    /// <summary>One real GC pass; the pins prevent any trim.</summary>
    [Benchmark]
    public Task<LatticeWalGcReport> BlockedPass() => _gc.RunOnceAsync("census-bench");

    /// <summary>Disposes the per-run services.</summary>
    [GlobalCleanup]
    public void Cleanup() => _services.Dispose();

    private sealed class PinGrain(IReadOnlyDictionary<string, HybridLogicalClock> pins) : IWalMaterialiserPinGrain
    {
        private readonly Task<IReadOnlyDictionary<string, HybridLogicalClock>> _pins = Task.FromResult(pins);
        private readonly Task<IReadOnlyDictionary<string, long>> _offsets =
            Task.FromResult<IReadOnlyDictionary<string, long>>(new Dictionary<string, long>());

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() => _pins;
        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() => _offsets;
        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => throw new NotSupportedException();
        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => throw new NotSupportedException();
        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => throw new NotSupportedException();
        public Task RemoveAsync(string consumerId) => throw new NotSupportedException();
        public Task ClearAsync() => throw new NotSupportedException();
    }
}
