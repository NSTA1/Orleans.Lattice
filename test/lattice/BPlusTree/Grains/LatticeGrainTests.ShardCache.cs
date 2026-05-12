using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Per-activation IShardRootGrain reference cache invariant tests ---
    //
    // These tests pin the contract introduced when LatticeGrain.GetShardGrainAsync
    // grew a single-slot cache of the most-recent resolved IShardRootGrain
    // reference. The cache is per-activation; sibling [StatelessWorker]
    // activations carry independent caches and self-heal lazily on observed
    // staleness. The invariant the tests lock is: the cache is cleared in
    // EVERY routing-invalidation hook (TryInvalidateStaleAlias and
    // InvalidateShardMap), not just where _physicalTreeId / _shardMap are
    // nulled. Forgetting to clear it would silently route to the stale shard
    // on retry and either loop or return wrong-shard results.

    [Test]
    public async Task GetShardGrainAsync_caches_shard_reference_for_repeated_same_key()
    {
        const string treeId = "shard-cache-repeat";
        var (grain, factory) = CreateGrain(treeId);
        var shardRoot = SetupShardRoot(factory);

        await grain.GetAsync("k1");
        await grain.GetAsync("k1");
        await grain.GetAsync("k1");

        // Without the cache: factory.GetGrain<IShardRootGrain>(...) would be
        // called 3 times (once per request). With the cache: the first call
        // materialises and caches the reference; the next two are cache hits
        // and bypass the factory entirely.
        var shardIndex = LatticeGrain.GetShardIndex("k1", 4);
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/{shardIndex}", Arg.Any<string>());
    }

    [Test]
    public async Task GetShardGrainAsync_caches_per_shard_index()
    {
        const string treeId = "shard-cache-per-shard";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);

        // Pick two keys whose hashes route to distinct shards. Iterating
        // candidates makes the test robust to hash-function changes.
        string keyA = "alpha";
        string keyB = "beta";
        var shardA = LatticeGrain.GetShardIndex(keyA, 4);
        int candidate = 0;
        while (LatticeGrain.GetShardIndex(keyB, 4) == shardA && candidate < 1000)
        {
            keyB = $"beta-{candidate++}";
        }
        var shardB = LatticeGrain.GetShardIndex(keyB, 4);
        Assert.That(shardB, Is.Not.EqualTo(shardA),
            "test setup failure: could not find two keys hashing to different shards");

        // Cycle-11 contract: the cache is array-keyed by physical shard
        // index, so each distinct shard is materialised exactly ONCE per
        // activation regardless of call interleaving. Alternation between
        // keyA and keyB no longer thrashes the cache - every call after the
        // first per-shard miss is a hit.
        await grain.GetAsync(keyA);
        await grain.GetAsync(keyA); // cache hit on shardA
        await grain.GetAsync(keyB); // cache miss on shardB (different array slot, no eviction)
        await grain.GetAsync(keyB); // cache hit on shardB
        await grain.GetAsync(keyA); // cache hit on shardA (still cached)

        // shardA and shardB each materialised exactly once.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/{shardA}", Arg.Any<string>());
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/{shardB}", Arg.Any<string>());
    }

    [Test]
    public async Task Stale_alias_invalidation_clears_shard_reference_cache()
    {
        // The strongest cache-clear test: after a stale-alias is observed,
        // the alias re-resolves to a DIFFERENT physical tree id. The retry
        // must materialise a shard reference under the new physical tree,
        // not re-use the stale cached one. If TryInvalidateStaleAlias did
        // not clear _cachedShard, the retry would short-circuit on the cache
        // and call the stale "phys-1/{shard}" reference forever.
        const string aliasId = "alias-stale-cache";
        const string oldPhysical = "phys-old";
        const string newPhysical = "phys-new";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId);

        var resolveCount = 0;
        registry.ResolveAsync(aliasId).Returns(_ =>
            Task.FromResult(resolveCount++ == 0 ? oldPhysical : newPhysical));

        // The substituted factory returns a single shared shardRoot for any
        // grain id, so we observe the cache-clear via factory.GetGrain call
        // counts on the two distinct keys (oldPhysical vs newPhysical paths).
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(shardRoot);

        var shardCallCount = 0;
        shardRoot.GetAsync("k1").Returns(_ =>
        {
            if (shardCallCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.FromResult<byte[]?>([42]);
        });

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.EqualTo(new byte[] { 42 }));
        var shardIndex = LatticeGrain.GetShardIndex("k1", 4);
        // Initial resolution materialised under the OLD physical tree id.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{oldPhysical}/{shardIndex}", Arg.Any<string>());
        // Retry must have materialised under the NEW physical tree id -
        // proving _cachedShard was cleared by TryInvalidateStaleAlias.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{newPhysical}/{shardIndex}", Arg.Any<string>());
    }

    [Test]
    public async Task Stale_shard_map_invalidation_clears_shard_reference_cache()
    {
        // Companion test for InvalidateShardMap. A StaleShardRoutingException
        // observed on shard 0 must cause the retry to route through a fresh
        // shard map (which now points the same key at shard 1) and materialise
        // the new shard reference. If InvalidateShardMap did not clear
        // _cachedShard, the retry would return the still-cached shard-0
        // reference and loop on the same StaleShardRoutingException.
        const string treeId = "shard-cache-stale-map";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        // Map fetched twice: first call returns map0 (slot 0 owned by shard 0);
        // after invalidation, second call returns map1 (slot 0 owned by shard 1).
        var map0 = new ShardMap { Slots = [0, 0], Version = 1 };
        var map1 = new ShardMap { Slots = [1, 1], Version = 2 };
        var mapCallCount = 0;
        registry.GetShardMapAsync(treeId).Returns(_ =>
            Task.FromResult<ShardMap?>(mapCallCount++ == 0 ? map0 : map1));

        var shardRoot0 = Substitute.For<IShardRootGrain>();
        var shardRoot1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>())
            .Returns(shardRoot0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>())
            .Returns(shardRoot1);

        // shardRoot0 throws StaleShardRoutingException; shardRoot1 succeeds.
        shardRoot0.GetAsync("k1").Returns<Task<byte[]?>>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shardRoot1.GetAsync("k1").Returns(Task.FromResult<byte[]?>([7]));

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.EqualTo(new byte[] { 7 }));
        // Initial materialisation routed to shard 0 (per map0).
        factory.Received(1).GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>());
        // Retry must have materialised shard 1 (per map1) - proving
        // _cachedShard was cleared by InvalidateShardMap. Without the
        // clear, the cache hit would return shardRoot0 again and the
        // outer await would surface StaleShardRoutingException to the caller.
        factory.Received(1).GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>());
        await shardRoot1.Received(1).GetAsync("k1");
    }
}
