using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Prices the actual shard audit versus opt-in survey on one 128-key orphan,
/// with the first miss at index 16. Includes substitute grain-call overhead,
/// not network latency. Run the orphanedsurvey suite with 64 invocations.
/// </summary>
[MemoryDiagnoser]
public class OrphanedLeafSurveyBenchmarks
{
    private ShardRootGrain _grain = null!;
    private object[] _substitutes = [];

    /// <summary>Builds a routed A/C chain with an unreachable B between them.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "survey-benchmark/0"));
        var factory = Substitute.For<IGrainFactory>();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(new TreeRegistryEntry
        {
            MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1,
        });

        var rootId = GrainId.Create("internal", "survey-root");
        var ids = new[] { GrainId.Create("leaf", "a"), GrainId.Create("leaf", "b"), GrainId.Create("leaf", "c") };
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null, "c"], ChildIds = [ids[0], ids[2]], ChildrenAreLeaves = true,
        });
        root.GetChildIdsAsync().Returns(new List<GrainId> { ids[0], ids[2] });
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        var keys = Enumerable.Range(0, 128).Select(i => $"b{i:D3}").ToList();
        var live = keys.Take(16).Concat(keys.Skip(17).Where((_, i) => i % 2 == 0)).ToHashSet();
        var value = new byte[] { 1 };
        var leaves = new IBPlusLeafGrain[3];
        for (var i = 0; i < leaves.Length; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetReclaimProbeAsync().Returns(new LeafReclaimProbe
            {
                LowKeyInclusive = i == 0 ? null : i == 1 ? "b" : "c",
                HighKeyExclusive = i == 2 ? null : "c",
                PrevSibling = i == 0 ? null : ids[i - 1],
                NextSibling = i == 2 ? null : ids[i + 1],
                LiveRowCount = 128,
            });
            leaf.GetKeysAsync().Returns(keys);
            leaf.GetAsync(Arg.Any<string>()).Returns(call => Task.FromResult<byte[]?>(
                live.Contains(call.Arg<string>()) ? value : null));
            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            leaves[i] = leaf;
        }
        _substitutes = [context, factory, monitor, registry, root, .. leaves];
        _grain = new ShardRootGrain(context,
            new FakePersistentState<ShardRootState> { State = new() { RootNodeId = rootId } },
            factory, new LatticeOptionsResolver(factory, monitor),
            NullLogger<ShardRootGrain>.Instance,
            new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance));
    }

    /// <summary>Prevents substitute call history accumulating across iterations.</summary>
    [IterationCleanup]
    public void Cleanup()
    {
        foreach (var substitute in _substitutes) substitute.ClearReceivedCalls();
    }

    /// <summary>Default, unchanged first-miss audit.</summary>
    [Benchmark(Baseline = true)]
    public Task FirstMiss() => _grain.RepairOrphanedLeavesAsync(null, true);

    /// <summary>Opt-in full key census on the same tree and grain-call seam.</summary>
    [Benchmark]
    public Task Survey() => _grain.SurveyOrphanedLeavesAsync(null);
}
