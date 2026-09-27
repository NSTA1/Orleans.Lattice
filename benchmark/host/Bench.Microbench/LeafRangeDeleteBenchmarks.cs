using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures a four-key foreground delete on a freshly attached 2,048-row leaf.
/// Setup and snapshot encoding are outside measurement. Run with --suite leafrangedelete.
/// </summary>
[MemoryDiagnoser]
public class LeafRangeDeleteBenchmarks
{
    private const int DeletesPerInvoke = 16;
    private byte[] _frame = null!;
    private BPlusLeafGrain[] _leaves = null!;

    /// <summary>Encodes the immutable snapshot outside measurement.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _frame = LeafSnapshotCodec.Encode(Enumerable.Range(0, 2048)
            .Select(i => new LeafSnapshotRow($"k{i:D6}",
                LwwValue<byte[]>.Create(new byte[256], HybridLogicalClock.Zero))).ToArray());
    }

    /// <summary>Restores fresh activations for each measured iteration.</summary>
    [IterationSetup]
    public void ResetLeaves()
    {
        var factory = new FakeGrainFactory();
        var registry = new FakeLatticeRegistry();
        registry.SetDefaultEntry(new TreeRegistryEntry { MaxLeafKeys = 100_000, ShardCount = 1 });
        factory.RouteByString<ILatticeRegistry>(_ => registry);
        var options = new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions());
        _leaves = new BPlusLeafGrain[DeletesPerInvoke];
        for (var i = 0; i < _leaves.Length; i++)
        {
            var state = new FakePersistentState<LeafNodeState>();
            var leaf = new BPlusLeafGrain(
                new FakeGrainContext(GrainId.Create("leaf", $"range-delete-{i}")),
                state,
                factory,
                new LatticeOptionsResolver(factory, options),
                new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance),
                new DefaultLatticeOriginClusterIdResolver());
            if (!leaf.CacheForTest.TryAttachSnapshot(_frame, 16 * 1024))
                throw new InvalidOperationException("The benchmark requires a lazily attached snapshot.");
            // A restored leaf carries its persisted digest. Do not time the
            // legacy missing-digest rebuild alongside the foreground delete.
            state.State.ProjectionHash = leaf.ComputeFullProjectionHashFromState();
            if (!leaf.CacheForTest.TryAttachSnapshot(_frame, 16 * 1024)
                || leaf.CacheForTest.HydratedRowCount != 0)
                throw new InvalidOperationException("Measurement must start without resident rows.");
            _leaves[i] = leaf;
        }
    }

    /// <summary>Deletes four keys through the shipping foreground leaf method.</summary>
    [Benchmark(OperationsPerInvoke = DeletesPerInvoke)]
    public async Task<int> DeleteRangeAsync()
    {
        var deleted = 0;
        foreach (var leaf in _leaves)
        {
            var result = await leaf.DeleteRangeAsync("k001024", "k001028");
            if (result.Deleted != 4 || !result.PastRange)
                throw new InvalidOperationException("The benchmark must delete four keys and observe the upper bound.");
            deleted += result.Deleted;
        }
        return deleted;
    }
}
