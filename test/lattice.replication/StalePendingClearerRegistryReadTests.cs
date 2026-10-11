using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Registry round trips the stale-pending clearer pays. Both passes derive the
/// physical tree and its shard map from one <see cref="ILatticeRegistry.GetEntryAsync"/>
/// read, because <see cref="ILatticeRegistry.ResolveAsync"/> and
/// <see cref="ILatticeRegistry.GetShardMapAsync"/> are pure projections of it.
/// </summary>
[TestFixture]
public class StalePendingClearerRegistryReadTests
{
    private const string Tree = "stale-pending-registry-reads";
    private const string Physical = Tree + "-copy";

    [Test]
    public async Task Capture_reads_the_registry_entry_once_and_walks_the_physical_shards()
    {
        var (factory, registry, map) = Create();

        await StalePendingClearer.CapturePendingAsync(factory, Tree, "site-a", CancellationToken.None);

        await AssertSingleEntryReadAsync(factory, registry, map);
    }

    [Test]
    public async Task Clear_reads_the_registry_entry_once_and_walks_the_physical_shards()
    {
        var (factory, registry, map) = Create();

        var cleared = await StalePendingClearer.ClearAsync(
            factory,
            Tree,
            "site-a",
            new HashSet<Guid>(),
            new Dictionary<Guid, bool>(),
            CancellationToken.None);

        Assert.That(cleared, Is.Zero);
        await AssertSingleEntryReadAsync(factory, registry, map);
    }

    [Test]
    public async Task An_unregistered_tree_walks_the_default_shards_under_its_own_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Tree).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var shards = new List<string>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(call =>
        {
            shards.Add(call.ArgAt<string>(0));
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<Orleans.Runtime.GrainId?>(null));
            return shard;
        });

        await StalePendingClearer.CapturePendingAsync(factory, Tree, "site-a", CancellationToken.None);

        var expected = ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount)
            .GetPhysicalShardIndices()
            .Select(i => $"{Tree}/{i}");
        Assert.That(shards, Is.EquivalentTo(expected));
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
    }

    private static (IGrainFactory Factory, ILatticeRegistry Registry, ShardMap Map) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Tree).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = Physical, ShardCount = 2, ShardMap = map }));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(_ =>
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<Orleans.Runtime.GrainId?>(null));
            return shard;
        });
        return (factory, registry, map);
    }

    private static async Task AssertSingleEntryReadAsync(IGrainFactory factory, ILatticeRegistry registry, ShardMap map)
    {
        await registry.Received(1).GetEntryAsync(Tree);
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            factory.Received().GetGrain<IShardRootGrain>($"{Physical}/{shardIndex}");
        }

        factory.DidNotReceive().GetGrain<IShardRootGrain>(Arg.Is<string>(k => k.StartsWith(Tree + "/", StringComparison.Ordinal)));
    }
}
