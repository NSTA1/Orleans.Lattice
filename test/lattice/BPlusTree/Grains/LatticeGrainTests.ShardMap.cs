using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Shard map indirection tests ---

    private static (LatticeGrain grain, IGrainFactory factory, ILatticeRegistry registry)
        CreateGrainWithRegistry(
            string treeId,
            LatticeOptions? options = null,
            int shardCount = 4,
            int virtualShardCount = 16)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var baseOptions = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(baseOptions);

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = shardCount,
            }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, baseOptions);

        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver);
        return (grain, grainFactory, registry);
    }

    [Test]
    public async Task GetAsync_uses_default_shard_map_when_registry_returns_null()
    {
        const string treeId = "default-map";
        var (grain, factory, _) = CreateGrainWithRegistry(treeId, shardCount: 4, virtualShardCount: 16);
        var shardRoot = SetupShardRoot(factory);

        await grain.GetAsync("hello");

        // Default map: shard = (hash % 16) % 4 = hash % 4 = legacy routing.
        var legacyShard = LatticeGrain.GetShardIndex("hello", 4);
        factory.Received(1).GetGrain<IShardRootGrain>(
            Arg.Is<string>(s => s == $"{treeId}/{legacyShard}"),
            Arg.Any<string>());
    }

    [Test]
    public async Task GetAsync_uses_custom_shard_map_from_registry()
    {
        const string treeId = "custom-map";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, shardCount: 4, virtualShardCount: 8);

        // Retarget every virtual slot to physical shard 7 (e.g. simulating a split target).
        var customMap = new ShardMap { Slots = [7, 7, 7, 7, 7, 7, 7, 7] };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(customMap));

        SetupShardRoot(factory);
        await grain.GetAsync("any-key");

        factory.Received(1).GetGrain<IShardRootGrain>(
            Arg.Is<string>(s => s == $"{treeId}/7"),
            Arg.Any<string>());
    }

    [Test]
    public async Task GetAsync_caches_shard_map_per_activation()
    {
        const string treeId = "cached-map";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId);
        SetupShardRoot(factory);

        await grain.GetAsync("k1");
        await grain.GetAsync("k2");
        await grain.GetAsync("k3");

        // Map fetched at most once per activation.
        await registry.Received(1).GetShardMapAsync(treeId);
    }

    [Test]
    public async Task GetAsync_refreshes_shard_map_on_stale_alias()
    {
        const string treeId = "refresh-map";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId);

        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var callCount = 0;
        shardRoot.GetAsync("k1").Returns(_ =>
        {
            if (callCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult<byte[]?>([1]);
        });

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.EqualTo(new byte[] { 1 }));
        // Map fetched twice — once initially, once after invalidation.
        await registry.Received(2).GetShardMapAsync(treeId);
    }

    [Test]
    public async Task System_tree_uses_default_shard_map_without_registry_lookup()
    {
        // System trees resolve to LatticeConstants.DefaultShardCount, so
        // the base options must supply a compatible VirtualShardCount.
        var (grain, factory, registry) = CreateGrainWithRegistry(
            LatticeConstants.RegistryTreeId,
            virtualShardCount: LatticeConstants.DefaultShardCount);
        SetupShardRoot(factory);

        await grain.GetAsync("k1");

        // System trees must never call the registry (would deadlock the bootstrap).
        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [Test]
    public async Task CountPerShardAsync_uses_physical_shard_indices_from_map()
    {
        const string treeId = "fan-out";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, shardCount: 4, virtualShardCount: 8);

        // Custom map with only physical shards 0 and 2 referenced.
        var customMap = new ShardMap { Slots = [0, 0, 2, 2, 0, 2, 0, 2] };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(customMap));

        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);
        shardRoot.CountAsync().Returns(5);

        var counts = await grain.CountPerShardAsync();

        // Should fan out to exactly the two distinct physical shards (0 and 2).
        Assert.That(counts, Has.Count.EqualTo(2));
        factory.Received(1).GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>());
        factory.Received(1).GetGrain<IShardRootGrain>($"{treeId}/2", Arg.Any<string>());
        factory.DidNotReceive().GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>());
        factory.DidNotReceive().GetGrain<IShardRootGrain>($"{treeId}/3", Arg.Any<string>());
    }
}
