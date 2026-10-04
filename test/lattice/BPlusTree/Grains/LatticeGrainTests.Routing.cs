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

    [Test]
    public async Task GetRoutingAsync_does_not_publish_a_pair_read_before_an_invalidation()
    {
        // A resolve that read the registry before an invalidation must not
        // publish what it read once it finishes, even when nothing newer is
        // published by then: the invalidation is the only record that the pair
        // went stale (RoutingPairPublishGate's epoch arm; shard-ownership review
        // #4435, finding F9). The interleaving: a slow resolve reads the row that
        // names the old copy; a forced refresh invalidates and resolves the new
        // copy and publishes it; then the slow resolve finishes.
        const string aliasId = "routing-epoch";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        SetupShardRoot(factory);
        var staleRead = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var freshRead = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var answers = new Queue<Task<string>>(new[] { staleRead.Task, freshRead.Task });
        registry.ResolveAsync(aliasId).Returns(_ => answers.Count > 0 ? answers.Dequeue() : Task.FromResult("physical-new"));

        var slow = grain.GetRoutingAsync(forceRefresh: false);
        var forced = grain.GetRoutingAsync(forceRefresh: true);
        freshRead.SetResult("physical-new");
        Assert.That((await forced).PhysicalTreeId, Is.EqualTo("physical-new"), "precondition: the refresh published the new copy");
        staleRead.SetResult("physical-old");
        Assert.That((await slow).PhysicalTreeId, Is.EqualTo("physical-old"), "the slow caller still gets the pair it read");

        var next = await grain.GetRoutingAsync(forceRefresh: false);

        Assert.That(next.PhysicalTreeId, Is.EqualTo("physical-new"),
            "a pair read before the invalidation must not overwrite the one published after it");
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
        var afterFirst = RoutingResolves(registry, nameof(ILatticeRegistry.GetShardMapAsync));
        var second = await grain.GetRoutingAsync(forceRefresh: false);

        Assert.That(second.Map, Is.SameAs(first.Map));
        Assert.That(RoutingResolves(registry, nameof(ILatticeRegistry.GetShardMapAsync)), Is.EqualTo(afterFirst));
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
    public async Task GetRoutingAsync_forceRefresh_true_also_invalidates_cached_alias()
    {
        // Regression: a caller invoking forceRefresh:true is by definition
        // trying to escape a StaleTreeRoutingException retry loop, which
        // is an alias-level concern. Before the fix this overload only
        // invalidated the shard map; the cached alias persisted across
        // refreshes so the next resolve handed back the same stale
        // physical tree id and the caller spun against the same throw
        // indefinitely (observed locally as 10,267 stale-tree retries in
        // 30 seconds inside AtomicWriteGrain.CaptureShardAsync during a
        // mid-saga online resize).
        //
        // The contract now is: forceRefresh:true clears BOTH the cached
        // alias and the cached shard map, so the next routing fetch
        // re-resolves the alias from the registry. Stubbing the registry
        // to return two distinct physical tree ids proves the second
        // call returned the freshly-resolved one.
        const string aliasId = "alias-force-refresh-invalidates";
        const string physicalV1 = "physical-v1";
        const string physicalV2 = "physical-v2";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        registry.ResolveAsync(aliasId).Returns(
            Task.FromResult(physicalV1),
            Task.FromResult(physicalV2));
        SetupShardRoot(factory);

        var first = await grain.GetRoutingAsync(forceRefresh: false);
        Assert.That(first.PhysicalTreeId, Is.EqualTo(physicalV1));
        var afterFirst = RoutingResolves(registry, nameof(ILatticeRegistry.ResolveAsync));

        var second = await grain.GetRoutingAsync(forceRefresh: true);

        Assert.That(second.PhysicalTreeId, Is.EqualTo(physicalV2),
            "forceRefresh:true must invalidate the cached alias so the next resolve picks up the new physical tree id.");
        Assert.That(RoutingResolves(registry, nameof(ILatticeRegistry.ResolveAsync)), Is.EqualTo(afterFirst + 1));
    }

    [Test]
    public async Task GetRoutingAsync_forceRefresh_true_invalidates_alias_and_map_together()
    {
        // Combined invariant: a single forceRefresh:true call must
        // invalidate the cached alias AND the cached shard map in the
        // same step, so a stale-routing escape path that goes through
        // this overload converges in one fetch even when both
        // dimensions have shifted (e.g. online resize that swaps the
        // alias AND a coincident adaptive shard split that remaps a
        // virtual slot).
        const string aliasId = "alias-combined-refresh";
        const string physicalV1 = "phys-combined-v1";
        const string physicalV2 = "phys-combined-v2";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId, shardCount: 4, virtualShardCount: 8);
        registry.ResolveAsync(aliasId).Returns(
            Task.FromResult(physicalV1),
            Task.FromResult(physicalV2));
        var mapV1 = new ShardMap { Slots = [0, 1, 2, 3, 0, 1, 2, 3] };
        var mapV2 = new ShardMap { Slots = [3, 2, 1, 0, 3, 2, 1, 0] };
        registry.GetShardMapAsync(aliasId).Returns(
            Task.FromResult<ShardMap?>(mapV1),
            Task.FromResult<ShardMap?>(mapV2));
        SetupShardRoot(factory);

        var first = await grain.GetRoutingAsync(forceRefresh: false);
        Assert.That(first.PhysicalTreeId, Is.EqualTo(physicalV1));
        Assert.That(first.Map, Is.SameAs(mapV1));

        var second = await grain.GetRoutingAsync(forceRefresh: true);

        Assert.That(second.PhysicalTreeId, Is.EqualTo(physicalV2),
            "forceRefresh:true must re-resolve the alias.");
        Assert.That(second.Map, Is.SameAs(mapV2),
            "forceRefresh:true must re-fetch the shard map.");
        await registry.Received(2).ResolveAsync(aliasId);
        await registry.Received(2).GetShardMapAsync(aliasId);
    }

    [Test]
    public async Task GetRoutingAsync_forceRefresh_true_does_not_loop_when_alias_unchanged()
    {
        // The forceRefresh-invalidates-alias contract must not regress
        // into an infinite loop when the alias has NOT actually
        // changed. Two successive forceRefresh:true calls against a
        // registry that consistently resolves to the same physical
        // tree id must return that id both times, paying exactly two
        // ResolveAsync hits (one per forced refresh) - not zero (which
        // would imply the alias was wrongly preserved) and not three
        // or more (which would imply runaway re-resolution inside a
        // single call).
        const string aliasId = "alias-stable";
        const string physicalId = "phys-stable";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);
        registry.ResolveAsync(aliasId).Returns(Task.FromResult(physicalId));
        SetupShardRoot(factory);

        _ = await grain.GetRoutingAsync(forceRefresh: false);
        var afterFirst = RoutingResolves(registry, nameof(ILatticeRegistry.ResolveAsync));
        var second = await grain.GetRoutingAsync(forceRefresh: true);
        var third = await grain.GetRoutingAsync(forceRefresh: true);

        Assert.That(second.PhysicalTreeId, Is.EqualTo(physicalId));
        Assert.That(third.PhysicalTreeId, Is.EqualTo(physicalId));
        Assert.That(RoutingResolves(registry, nameof(ILatticeRegistry.ResolveAsync)), Is.EqualTo(afterFirst + 2));
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
        registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }
}

