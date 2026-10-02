using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression coverage for which shard roots
/// <see cref="LatticeStateQuery.GetTreeStructureAsync"/> reports on a
/// whole-tree read: every physical shard the tree's routing map sends keys to.
/// It used to walk <c>0..ShardCount-1</c> of the pinned count, which an
/// adaptive split does not change when it moves slots to a shard above it, so
/// the split target - and every key routed there - was missing from the
/// structure.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryStructureRoutingTests
{
    private const string Tree = "orders";
    private const string PhysicalTree = "orders-resized-1";

    private sealed class Harness
    {
        public required LatticeStateQuery Query { get; init; }
        public required ILattice Lattice { get; init; }
        public required Dictionary<int, IShardRootGrain> Shards { get; init; }
    }

    private static Harness CreateQuery(ShardMap map, int pinnedShardCount, params int[] shardsWithRoots)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Tree).Returns(Task.FromResult(PhysicalTree));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = pinnedShardCount,
        }));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var lattice = Substitute.For<ILattice>();
        lattice.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        lattice.GetRoutingAsync(true, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo(PhysicalTree, map)));
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);

        var shards = new Dictionary<int, IShardRootGrain>();
        foreach (var index in shardsWithRoots)
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetTopologySnapshotAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new ShardTopologyNode
                {
                    NodeId = $"leaf-{index}",
                    IsLeaf = true,
                    ShardIndex = index,
                    SubtreeDepth = 1,
                    EntryCount = 10 + index,
                    LiveCount = 10 + index,
                }));
            grainFactory.GetGrain<IShardRootGrain>($"{PhysicalTree}/{index}").Returns(shard);
            shards[index] = shard;
        }

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var query = new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            new ServiceCollection().BuildServiceProvider(),
            new NullTenantContextResolver());

        return new Harness { Query = query, Lattice = lattice, Shards = shards };
    }

    [Test]
    public async Task Whole_tree_structure_includes_a_split_target_above_the_pinned_shard_count()
    {
        // Pinned at two shards; an adaptive split moved slots 3 and 6 to shard 5.
        var map = new ShardMap { Slots = [0, 1, 0, 5, 0, 1, 5, 1], Version = 4 };
        var h = CreateQuery(map, pinnedShardCount: 2, 0, 1, 5);

        var result = await h.Query.GetTreeStructureAsync(new StructureRequest { TreeId = Tree });

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Roots.Select(r => r.ShardIndex), Is.EqualTo(new[] { 0, 1, 5 }));
            Assert.That(result.Roots.Single(r => r.ShardIndex == 5).SubtreeKeyCount, Is.EqualTo(15));
        });
    }

    [Test]
    public async Task Whole_tree_structure_omits_a_pinned_index_the_routing_map_no_longer_reaches()
    {
        // A consolidation retired physical shard 1 from the map; it no longer
        // serves any key, so it is not a root of the tree's structure.
        var map = new ShardMap { Slots = [0, 2, 0, 2], Version = 7 };
        var h = CreateQuery(map, pinnedShardCount: 3, 0, 2);

        var result = await h.Query.GetTreeStructureAsync(new StructureRequest { TreeId = Tree });

        Assert.That(result.Roots.Select(r => r.ShardIndex), Is.EqualTo(new[] { 0, 2 }));
    }

    [Test]
    public async Task Structure_for_an_explicit_shard_reads_only_that_shard_without_routing()
    {
        var map = ShardMap.CreateDefault(8, 2);
        var h = CreateQuery(map, pinnedShardCount: 2, 0, 1, 5);

        var result = await h.Query.GetTreeStructureAsync(new StructureRequest { TreeId = Tree, ShardIndex = 5 });

        Assert.That(result.Roots.Select(r => r.ShardIndex), Is.EqualTo(new[] { 5 }));
        _ = h.Lattice.DidNotReceive().GetRoutingAsync(Arg.Any<CancellationToken>());
        _ = h.Lattice.DidNotReceive().GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await h.Shards[0].DidNotReceive().GetTopologySnapshotAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
    }
}
