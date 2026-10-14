using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class HotShardMonitorGrainTests
{
    private static readonly HashSet<string> RegistryReadMembers = new(StringComparer.Ordinal)
    {
        nameof(ILatticeRegistry.GetEntryAsync),
        nameof(ILatticeRegistry.GetEntriesAsync),
        nameof(ILatticeRegistry.ResolveAsync),
        nameof(ILatticeRegistry.GetShardMapAsync),
        nameof(ILatticeRegistry.ExistsAsync),
    };

    private static int CountRegistryReads(ILatticeRegistry registry) =>
        registry.ReceivedCalls().Count(c => RegistryReadMembers.Contains(c.GetMethodInfo().Name));

    [Test]
    public async Task RunSamplingPass_reads_the_physical_id_and_map_in_one_registry_read()
    {
        // One read observes the tree is registered before anything else; the
        // physical id and the shard map are then projected from a single entry
        // read rather than a resolve followed by a map read.
        var (grain, _, _, _, registry, _, _) = CreateGrain();

        await grain.RunSamplingPassAsync();

        Assert.That(CountRegistryReads(registry), Is.EqualTo(2));
        await registry.Received(2).GetEntryAsync(TreeId);
        await registry.DidNotReceiveWithAnyArgs().ResolveAsync(default!);
        await registry.DidNotReceiveWithAnyArgs().GetShardMapAsync(default!);
    }

    [Test]
    public async Task RunSamplingPass_samples_the_aliased_physical_tree_from_the_entry()
    {
        const string Physical = "hot-physical";
        var (grain, grainFactory, _, _, registry, _, _) = CreateGrain(physicalShardCount: 2, virtualShardCount: 16);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = 2,
                ShardMap = ShardMap.CreateDefault(16, 2),
                PhysicalTreeId = Physical,
            }));

        await grain.RunSamplingPassAsync();

        grainFactory.Received().GetGrain<IShardRootGrain>($"{Physical}/0");
        grainFactory.Received().GetGrain<IShardRootGrain>($"{Physical}/1");
        grainFactory.DidNotReceive().GetGrain<IShardRootGrain>($"{TreeId}/0");
    }
}
