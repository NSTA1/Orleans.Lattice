using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- GetRoutingAsync tests ---

    [Test]
    public async Task GetRoutingAsync_returns_default_map_when_registry_returns_null()
    {
        const string treeId = "routing-default";
        var (grain, factory, _) = CreateGrainWithRegistry(treeId, shardCount: 4);
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing, Is.Not.Null);
        Assert.That(routing.PhysicalTreeId, Is.EqualTo(treeId));
        Assert.That(routing.Map, Is.Not.Null);
        Assert.That(routing.Map.Slots.Length, Is.EqualTo(LatticeConstants.DefaultVirtualShardCount));
        for (int i = 0; i < LatticeConstants.DefaultVirtualShardCount; i++)
            Assert.That(routing.Map.Slots[i], Is.EqualTo(i % 4));
    }

    [Test]
    public async Task GetRoutingAsync_returns_custom_map_from_registry()
    {
        const string treeId = "routing-custom";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, shardCount: 4, virtualShardCount: 8);
        var customMap = new ShardMap { Slots = [0, 1, 2, 3, 0, 1, 2, 3] };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(customMap));
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing.Map, Is.SameAs(customMap));
        Assert.That(routing.PhysicalTreeId, Is.EqualTo(treeId));
    }

    [Test]
    public async Task GetRoutingAsync_resolves_alias_to_physical_tree_id()
    {
        const string aliasId = "alias-tree";
        const string physicalId = "physical-tree";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        registry.ResolveAsync(aliasId).Returns(Task.FromResult(physicalId));
        SetupShardRoot(factory);

        var routing = await grain.GetRoutingAsync();

        Assert.That(routing.PhysicalTreeId, Is.EqualTo(physicalId));
    }

    [Test]
    public async Task GetRoutingAsync_supports_tuple_deconstruction()
    {
        const string treeId = "routing-deconstruct";
        var (grain, factory, _) = CreateGrainWithRegistry(treeId);
        SetupShardRoot(factory);

        var (physicalId, map) = await grain.GetRoutingAsync();

        Assert.That(physicalId, Is.EqualTo(treeId));
        Assert.That(map, Is.Not.Null);
    }

    // --- GetRoutingAsync force-refresh overload tests ---

    [Test]
    public async Task GetRoutingAsync_forceRefresh_false_uses_cached_shard_map()
    {
        // First call resolves the shard map from the registry; the
        // second call with forceRefresh=false must hit the per-
        // activation cache without re-querying GetShardMapAsync.
        const string treeId = "routing-cached";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, shardCount: 4);
        SetupShardRoot(factory);

        var first = await grain.GetRoutingAsync(forceRefresh: false);
        var second = await grain.GetRoutingAsync(forceRefresh: false);

        Assert.That(second.Map, Is.SameAs(first.Map));
        await registry.Received(1).GetShardMapAsync(treeId);
    }

    [Test]
    public async Task GetRoutingAsync_forceRefresh_true_invalidates_cache_and_refetches()
    {
        // Force-refresh must invalidate the cached ShardMap so the next
        // resolution re-queries the registry. Stubbing the registry to
        // return two distinct maps proves the second call returned the
        // freshly-fetched one.
        const string treeId = "routing-force-refresh";
        var (grain, factory, registry) = CreateGrainWithRegistry(treeId, shardCount: 4, virtualShardCount: 8);
        SetupShardRoot(factory);

        var mapV1 = new ShardMap { Slots = [0, 1, 2, 3, 0, 1, 2, 3] };
        var mapV2 = new ShardMap { Slots = [3, 2, 1, 0, 3, 2, 1, 0] };
        registry.GetShardMapAsync(treeId).Returns(
            Task.FromResult<ShardMap?>(mapV1),
            Task.FromResult<ShardMap?>(mapV2));

        var first = await grain.GetRoutingAsync(forceRefresh: false);
        Assert.That(first.Map, Is.SameAs(mapV1));

        var second = await grain.GetRoutingAsync(forceRefresh: true);

        Assert.That(second.Map, Is.SameAs(mapV2));
        await registry.Received(2).GetShardMapAsync(treeId);
    }

    [Test]
    public async Task GetRoutingAsync_forceRefresh_true_preserves_resolved_physical_tree_id()
    {
        // Force-refresh invalidates the shard map but must NOT bust the
        // resolved physical tree id - alias swaps go through a separate
        // catch-clause path (TryInvalidateStaleAlias). The post-refresh
        // PhysicalTreeId must therefore equal the pre-refresh value
        // without re-calling ResolveAsync.
        const string aliasId = "alias-preserve";
        const string physicalId = "physical-preserve";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        registry.ResolveAsync(aliasId).Returns(Task.FromResult(physicalId));
        SetupShardRoot(factory);

        var first = await grain.GetRoutingAsync(forceRefresh: false);
        Assert.That(first.PhysicalTreeId, Is.EqualTo(physicalId));

        var second = await grain.GetRoutingAsync(forceRefresh: true);

        Assert.That(second.PhysicalTreeId, Is.EqualTo(physicalId));
        await registry.Received(1).ResolveAsync(aliasId);
    }

    [Test]
    public void GetRoutingAsync_forceRefresh_observes_cancellation_token()
    {
        // A pre-cancelled token must short-circuit the overload before
        // any registry call or invalidation runs.
        const string treeId = "routing-cancellation";
        var (grain, _, registry) = CreateGrainWithRegistry(treeId);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(async () => await grain.GetRoutingAsync(forceRefresh: true, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        // The registry should never have been consulted - confirm the
        // cancellation check fires before any RPC fan-out.
        registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
    }
}

