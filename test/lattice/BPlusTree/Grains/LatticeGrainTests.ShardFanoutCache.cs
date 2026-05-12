using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeGrainTests
{
    // --- Per-activation IShardRootGrain reference cache: bulk-fanout sites ---
    //
    // Cycle 10 extended the cycle-8 single-slot cache (populated by
    // GetShardGrainAsync on the single-key path) to the seven hot-path
    // bulk-fanout call sites in LatticeGrain via a new private helper
    // GetShardGrainByIndex(physicalTreeId, shardIndex). Cycle 11 promoted
    // the single-slot _cachedShard/_cachedShardIndex fields to an array-
    // keyed IShardRootGrain?[] _cachedShards indexed by physical shard
    // index, removing the thrash on multi-shard fanout, and extended the
    // helper to four further KeysAsync/EntriesAsync sites. These tests
    // pin the observable contract:
    //
    //   1. Repeat-shard calls on a bulk-fanout path (e.g. SetManyAsync at
    //      ShardCount=1) materialise the IShardRootGrain reference exactly
    //      once across multiple iterations.
    //   2. The cache is shared between the single-key path and the
    //      bulk-fanout path: a SetAsync that warms the cache makes a
    //      subsequent SetManyAsync to the same shard a cache hit.
    //   3. Multi-shard fanout (ShardCount > 1) materialises each shard
    //      reference exactly once across an entire fan-out call (cycle-11
    //      array-keyed contract; cycle-10 single-slot would thrash here).
    //   4. DeleteRange (every-shard fanout) reuses the cached reference
    //      when the tree is sharded down to a single physical shard.
    //   5. KeysAsync / EntriesAsync open one cursor per physical shard at
    //      scan start; under the array-keyed cache each shard is
    //      materialised exactly once across the scan, and a warmed cache
    //      from the single-key path eliminates the materialisation
    //      entirely on the cursor-open call.
    //   6. Stale-alias invalidation observed on a bulk-fanout path clears
    //      the shared cache, so the retry materialises under the new
    //      physical tree id.
    //   7. Stale-shard-map invalidation observed on a bulk-fanout path
    //      clears the shared cache, so the retry materialises the new
    //      physical shard.

    [Test]
    public async Task SetManyAsync_caches_shard_reference_for_repeated_calls_at_single_shard()
    {
        const string treeId = "fanout-cache-repeat";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        SetupShardRoot(factory);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
            new("k2", [2]),
            new("k3", [3]),
        };

        await grain.SetManyAsync(batch);
        await grain.SetManyAsync(batch);
        await grain.SetManyAsync(batch);

        // ShardCount = 1 collapses the per-shard fan-out to a single bucket
        // per call. Without the bulk-path cache: 3 SetManyAsync calls would
        // mean 3 GetGrain<IShardRootGrain> materialisations. With the cache
        // shared via GetShardGrainByIndex: the first call materialises and
        // caches; the next two are cache hits.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
    }

    [Test]
    public async Task SetManyAsync_shares_cache_with_single_key_path()
    {
        const string treeId = "fanout-cache-shared-slot";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        SetupShardRoot(factory);

        // Warm the cache via the cycle-8 single-key path (GetShardGrainAsync).
        await grain.SetAsync("warmup", [0]);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
            new("k2", [2]),
        };

        // The bulk path must hit the same _cachedShard slot. If
        // GetShardGrainByIndex did not consult the slot, we would see a
        // second materialisation here.
        await grain.SetManyAsync(batch);
        await grain.SetManyAsync(batch);

        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
    }

    [Test]
    public async Task SetManyAsync_multi_shard_fanout_materialises_each_owner()
    {
        const string treeId = "fanout-cache-multi";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        // Pin a 2-slot map: slot 0 → shard 0, slot 1 → shard 1.
        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shardRoot0 = Substitute.For<IShardRootGrain>();
        var shardRoot1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>())
            .Returns(shardRoot0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>())
            .Returns(shardRoot1);

        // Find one key routing to slot 0 and another to slot 1 against
        // this 2-slot map. Robust to hash-function changes.
        string keyA = "alpha";
        int slotA = LatticeGrain.GetShardIndex(keyA, 2);
        string keyB = "beta";
        int candidate = 0;
        while (LatticeGrain.GetShardIndex(keyB, 2) == slotA && candidate < 2000)
            keyB = $"beta-{candidate++}";
        Assert.That(LatticeGrain.GetShardIndex(keyB, 2), Is.Not.EqualTo(slotA),
            "test setup failure: could not find two keys routing to different virtual slots");

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(keyA, [1]),
            new(keyB, [2]),
        };

        await grain.SetManyAsync(batch);

        // A single SetManyAsync over both shards must materialise each
        // owner at least once. (The companion test
        // SetManyAsync_multi_shard_fanout_materialises_each_owner_exactly_once
        // pins the cycle-11 array-keyed contract that the materialisation
        // is exactly once per owner.)
        factory.Received().GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
        factory.Received().GetGrain<IShardRootGrain>(
            $"{treeId}/1", Arg.Any<string>());
        await shardRoot0.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
        await shardRoot1.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task DeleteRangeAsync_reuses_cached_shard_reference_at_single_shard()
    {
        const string treeId = "fanout-cache-deleterange";
        var (grain, factory) = CreateGrain(treeId, shardCount: 1);
        var shardRoot = SetupShardRoot(factory);
        shardRoot.DeleteRangeAsync("a", "z").Returns(0);

        await grain.DeleteRangeAsync("a", "z");
        await grain.DeleteRangeAsync("a", "z");
        await grain.DeleteRangeAsync("a", "z");

        // DeleteRange fans out to every physical shard; with shardCount=1
        // that is the same single shard each call. The bulk-path cache
        // must collapse the three calls to a single materialisation.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
    }

    [Test]
    public async Task SetManyAsync_stale_alias_invalidation_clears_bulk_path_cache()
    {
        // Companion to ShardCache.cs's stale-alias test, but exercises the
        // bulk-fanout entry point. After a stale-alias is observed the
        // alias re-resolves to a DIFFERENT physical tree id; the retry
        // must materialise the new shard reference under the new physical
        // tree, not re-use the stale cached one.
        const string aliasId = "fanout-cache-alias-stale";
        const string oldPhysical = "phys-old";
        const string newPhysical = "phys-new";
        var (grain, factory, registry) = CreateGrainWithRegistry(aliasId, shardCount: 1);

        var resolveCount = 0;
        registry.ResolveAsync(aliasId).Returns(_ =>
            Task.FromResult(resolveCount++ == 0 ? oldPhysical : newPhysical));

        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(shardRoot);

        var setManyCallCount = 0;
        shardRoot.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(_ =>
        {
            if (setManyCallCount++ == 0)
                throw new InvalidOperationException("This tree has been deleted.");
            return Task.CompletedTask;
        });

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
        };

        await grain.SetManyAsync(batch);

        // Initial materialisation under the OLD physical tree id...
        factory.Received().GetGrain<IShardRootGrain>(
            $"{oldPhysical}/0", Arg.Any<string>());
        // ...retry under the NEW physical tree id, proving the cache slot
        // was cleared by TryInvalidateStaleAlias.
        factory.Received().GetGrain<IShardRootGrain>(
            $"{newPhysical}/0", Arg.Any<string>());
    }

    [Test]
    public async Task SetManyAsync_stale_shard_map_invalidation_clears_bulk_path_cache()
    {
        // Companion to ShardCache.cs's stale-shard-map test, but exercises
        // the bulk-fanout entry point. A StaleShardRoutingException
        // observed inside the per-shard fan-out must cause the retry to
        // route through a fresh shard map and materialise the new shard
        // reference. Without InvalidateShardMap clearing _cachedShard,
        // the retry would return the still-cached stale reference and
        // loop on the same exception.
        const string treeId = "fanout-cache-stale-map";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

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

        shardRoot0.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns<Task>(_ =>
            throw new StaleShardRoutingException(
                sourceShardIndex: 0, targetShardIndex: 1, virtualSlot: 0));
        shardRoot1.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
        };

        await grain.SetManyAsync(batch);

        // Initial materialisation routed to shard 0 (per map0)...
        factory.Received().GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>());
        // ...retry must have materialised shard 1 (per map1), proving the
        // bulk-path cache slot was cleared by InvalidateShardMap.
        factory.Received().GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>());
        await shardRoot1.Received(1).SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task SetManyAsync_multi_shard_fanout_materialises_each_owner_exactly_once()
    {
        // Companion to SetManyAsync_multi_shard_fanout_materialises_each_owner.
        // SetManyAsync buckets entries by slot internally, so within a SINGLE
        // call even cycle-10 single-slot would materialise each owner once
        // (the bucket-iteration order is deterministic). The cycle-11
        // array-keyed cache strengthens the contract ACROSS calls: a second
        // SetManyAsync to the same two-shard map must hit cache for both
        // owners. Under cycle-10 single-slot, the second call would miss
        // both slots (the slot was last set to whichever shard was
        // materialised second) and re-materialise both. Two-call totals:
        // cycle-10 = 4 materialisations; cycle-11 = 2.
        const string treeId = "fanout-cache-multi-exactly-once";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shardRoot0 = Substitute.For<IShardRootGrain>();
        var shardRoot1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>())
            .Returns(shardRoot0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>())
            .Returns(shardRoot1);

        // Find one key per slot against this 2-slot map. Use the keys
        // verbatim in every batch so re-hashing through GetShardIndex
        // produces a stable slot assignment.
        string keyA = "alpha";
        int slotA = LatticeGrain.GetShardIndex(keyA, 2);
        string keyB = "beta";
        int candidate = 0;
        while (LatticeGrain.GetShardIndex(keyB, 2) == slotA && candidate < 2000)
            keyB = $"beta-{candidate++}";
        Assert.That(LatticeGrain.GetShardIndex(keyB, 2), Is.Not.EqualTo(slotA),
            "test setup failure: could not find two keys routing to different virtual slots");

        var batch = new List<KeyValuePair<string, byte[]>>
        {
            new(keyA, [1]),
            new(keyB, [2]),
        };

        // Two consecutive multi-shard fanouts. Cycle-10 single-slot would
        // re-materialise both shards on the second call (4 total); cycle-11
        // array-keyed must hit the cache and skip both materialisations
        // (2 total: one per shard for the lifetime of the activation).
        await grain.SetManyAsync(batch);
        await grain.SetManyAsync(batch);
        await grain.SetManyAsync(batch);

        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/1", Arg.Any<string>());
    }

    [Test]
    public async Task KeysAsync_reuses_cached_shard_references_across_fanout()
    {
        // Pins the cycle-11 cursor-open cache contract for KeysAsync. The
        // per-physical-shard cursor loop in LatticeGrain.Keys.cs:107 calls
        // GetShardGrainByIndex for every physical shard at scan start. A
        // second consecutive scan must hit the array-keyed cache for both
        // shards: total 1 materialisation per shard across two scans.
        const string treeId = "fanout-cache-keys-async";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shardRoot0 = Substitute.For<IShardRootGrain>();
        var shardRoot1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>())
            .Returns(shardRoot0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>())
            .Returns(shardRoot1);

        // Stub both forward and reverse page fetches to terminate the scan
        // immediately - empty page, HasMore=false. This is the minimum
        // wiring needed for the cursor-open path to run end-to-end.
        var emptyPage = new KeysPage { Keys = [], HasMore = false };
        shardRoot0.GetSortedKeysBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>())
            .Returns(Task.FromResult(emptyPage));
        shardRoot1.GetSortedKeysBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>())
            .Returns(Task.FromResult(emptyPage));

        await foreach (var _ in grain.KeysAsync()) { }
        await foreach (var _ in grain.KeysAsync()) { }

        // Without the cycle-11 array-keyed cache at the cursor-open site,
        // each scan would re-materialise both shards (4 calls total).
        // With the cache: each shard is materialised exactly once across
        // the lifetime of the grain activation.
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/1", Arg.Any<string>());
    }

    [Test]
    public async Task EntriesAsync_reuses_cached_shard_references_across_fanout()
    {
        // Symmetric to KeysAsync_reuses_cached_shard_references_across_fanout.
        // Pins the cycle-11 cursor-open cache contract for EntriesAsync at
        // LatticeGrain.Entries.cs:82. Two consecutive scans must produce
        // exactly one materialisation per physical shard.
        const string treeId = "fanout-cache-entries-async";
        var (grain, factory, registry) = CreateGrainWithRegistry(
            treeId, shardCount: 2, virtualShardCount: 2);

        var map = new ShardMap { Slots = [0, 1], Version = 1 };
        registry.GetShardMapAsync(treeId).Returns(Task.FromResult<ShardMap?>(map));

        var shardRoot0 = Substitute.For<IShardRootGrain>();
        var shardRoot1 = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{treeId}/0", Arg.Any<string>())
            .Returns(shardRoot0);
        factory.GetGrain<IShardRootGrain>($"{treeId}/1", Arg.Any<string>())
            .Returns(shardRoot1);

        var emptyPage = new EntriesPage { Entries = [], HasMore = false };
        shardRoot0.GetSortedEntriesBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>())
            .Returns(Task.FromResult(emptyPage));
        shardRoot1.GetSortedEntriesBatchAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<string?>())
            .Returns(Task.FromResult(emptyPage));

        await foreach (var _ in grain.EntriesAsync()) { }
        await foreach (var _ in grain.EntriesAsync()) { }

        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/0", Arg.Any<string>());
        factory.Received(1).GetGrain<IShardRootGrain>(
            $"{treeId}/1", Arg.Any<string>());
    }
}