using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

public class ShardMapTests
{
    [Test]
    public void CreateDefault_assigns_modulo_physical_count()
    {
        var map = ShardMap.CreateDefault(virtualShardCount: 8, physicalShardCount: 4);

        Assert.That(map.Slots.Length, Is.EqualTo(8));
        Assert.That(map.Slots, Is.EqualTo(new[] { 0, 1, 2, 3, 0, 1, 2, 3 }));
    }

    [Test]
    public void CreateDefault_throws_when_virtual_less_than_physical()
    {
        Assert.That(
            () => ShardMap.CreateDefault(virtualShardCount: 4, physicalShardCount: 8),
            Throws.ArgumentException);
    }

    [Test]
    public void CreateDefault_throws_when_virtual_count_not_positive()
    {
        Assert.That(
            () => ShardMap.CreateDefault(virtualShardCount: 0, physicalShardCount: 1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void CreateDefault_throws_when_physical_count_not_positive()
    {
        Assert.That(
            () => ShardMap.CreateDefault(virtualShardCount: 8, physicalShardCount: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Resolve_throws_when_key_null()
    {
        var map = ShardMap.CreateDefault(8, 4);
        Assert.That(() => map.Resolve(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_throws_when_map_empty()
    {
        var map = new ShardMap();
        Assert.That(() => map.Resolve("k"), Throws.InvalidOperationException);
    }

    [Test]
    public void Resolve_is_deterministic()
    {
        var map = ShardMap.CreateDefault(4096, 64);
        var a = map.Resolve("hello");
        var b = map.Resolve("hello");
        Assert.That(b, Is.EqualTo(a));
    }

    [Test]
    public void Resolve_default_map_matches_legacy_modulo_routing()
    {
        // When virtualShardCount % physicalShardCount == 0, the default
        // identity map must produce the same routing as the legacy
        // hash % physicalShardCount formula.
        var map = ShardMap.CreateDefault(4096, 64);
        var keys = new[] { "alpha", "beta", "customer-12345", "", "z", "foo:bar:baz" };
        foreach (var key in keys)
        {
            var legacy = LatticeGrain.GetShardIndex(key, 64);
            var viaMap = map.Resolve(key);
            Assert.That(viaMap, Is.EqualTo(legacy), $"Mismatch for key '{key}'");
        }
    }

    [Test]
    public void Resolve_routes_to_remapped_physical_shard()
    {
        var map = ShardMap.CreateDefault(8, 4);
        // Retarget every virtual slot to physical shard 7 (simulating a split).
        for (int i = 0; i < map.Slots.Length; i++)
            map.Slots[i] = 7;

        Assert.That(map.Resolve("anything"), Is.EqualTo(7));
    }

    [Test]
    public void GetVirtualSlot_throws_when_key_null()
    {
        Assert.That(() => ShardMap.GetVirtualSlot(null!, 8), Throws.ArgumentNullException);
    }

    [Test]
    public void GetVirtualSlot_throws_when_count_not_positive()
    {
        Assert.That(() => ShardMap.GetVirtualSlot("k", 0), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetVirtualSlot_stays_in_range()
    {
        for (int i = 0; i < 1000; i++)
        {
            var slot = ShardMap.GetVirtualSlot($"key-{i}", 4096);
            Assert.That(slot, Is.InRange(0, 4095));
        }
    }

    [Test]
    public void GetPhysicalShardIndices_returns_distinct_sorted_set()
    {
        var map = new ShardMap { Slots = [3, 1, 1, 0, 2, 2, 3, 0] };

        var result = map.GetPhysicalShardIndices();

        Assert.That(result, Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void GetPhysicalShardIndices_default_map_is_zero_to_n_minus_one()
    {
        var map = ShardMap.CreateDefault(64, 8);
        Assert.That(map.GetPhysicalShardIndices(), Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }));
    }

    [Test]
    public void GetPhysicalShardIndices_returns_same_instance_on_repeat_calls()
    {
        var map = ShardMap.CreateDefault(64, 8);
        var first = map.GetPhysicalShardIndices();
        var second = map.GetPhysicalShardIndices();
        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void GetPhysicalShardIndices_returns_empty_when_slots_empty()
    {
        var map = new ShardMap();
        Assert.That(map.GetPhysicalShardIndices(), Is.Empty);
    }

    [Test]
    public void GetPhysicalShardIndices_handles_single_slot()
    {
        var map = new ShardMap { Slots = [5] };
        Assert.That(map.GetPhysicalShardIndices(), Is.EqualTo(new[] { 5 }));
    }

    [Test]
    public void GetPhysicalShardIndices_handles_sparse_indices_beyond_stack_threshold()
    {
        // Forces the heap-rented bitmap branch: max index (1000) exceeds the
        // stackalloc threshold (256) but stays under the heap threshold.
        var map = new ShardMap { Slots = [0, 1000, 500, 1000, 0, 250] };
        Assert.That(
            map.GetPhysicalShardIndices(),
            Is.EqualTo(new[] { 0, 250, 500, 1000 }));
    }

    [Test]
    public void VirtualShardCount_reflects_slot_array_length()
    {
        var map = new ShardMap { Slots = new int[16] };
        Assert.That(map.VirtualShardCount, Is.EqualTo(16));
    }

    [Test]
    public void GetOrCreateDefaultShared_returns_same_instance_for_same_key()
    {
        // The shared cache is keyed by (virtualShardCount, physicalShardCount).
        // Two calls with the same key must return the same reference so the
        // ten ?? fallback callsites stop allocating a fresh 16 KB int[] per call.
        var a = ShardMap.GetOrCreateDefaultShared(4096, 4);
        var b = ShardMap.GetOrCreateDefaultShared(4096, 4);
        Assert.That(b, Is.SameAs(a));
    }

    [Test]
    public void GetOrCreateDefaultShared_returns_distinct_instances_for_distinct_keys()
    {
        var a = ShardMap.GetOrCreateDefaultShared(4096, 4);
        var bDifferentPhysical = ShardMap.GetOrCreateDefaultShared(4096, 8);
        var cDifferentVirtual = ShardMap.GetOrCreateDefaultShared(2048, 4);
        Assert.That(bDifferentPhysical, Is.Not.SameAs(a));
        Assert.That(cDifferentVirtual, Is.Not.SameAs(a));
        Assert.That(cDifferentVirtual, Is.Not.SameAs(bDifferentPhysical));
    }

    [Test]
    public void GetOrCreateDefaultShared_returns_identity_map_with_zero_version()
    {
        // The cached map must be functionally identical to a CreateDefault map:
        // Version 0, modulo-physical-count slots, matching VirtualShardCount.
        var map = ShardMap.GetOrCreateDefaultShared(8, 4);
        Assert.That(map.Version, Is.EqualTo(0L));
        Assert.That(map.VirtualShardCount, Is.EqualTo(8));
        Assert.That(map.Slots, Is.EqualTo(new[] { 0, 1, 2, 3, 0, 1, 2, 3 }));
    }

    [Test]
    public void GetOrCreateDefaultShared_shares_memoised_physical_shard_indices()
    {
        // Bonus property: because the cache shares the instance, the lazy
        // GetPhysicalShardIndices() result is computed once and observed by
        // every subsequent caller. This is a positive side-effect of the cache
        // and amortises the bool-bitmap dedup walk.
        var first = ShardMap.GetOrCreateDefaultShared(4096, 16);
        var firstIndices = first.GetPhysicalShardIndices();
        var second = ShardMap.GetOrCreateDefaultShared(4096, 16);
        var secondIndices = second.GetPhysicalShardIndices();
        Assert.That(secondIndices, Is.SameAs(firstIndices));
    }

    [Test]
    public void GetOrCreateDefaultShared_throws_when_virtual_count_not_positive()
    {
        Assert.That(
            () => ShardMap.GetOrCreateDefaultShared(virtualShardCount: 0, physicalShardCount: 1),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetOrCreateDefaultShared_throws_when_physical_count_not_positive()
    {
        Assert.That(
            () => ShardMap.GetOrCreateDefaultShared(virtualShardCount: 8, physicalShardCount: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetOrCreateDefaultShared_throws_when_virtual_less_than_physical()
    {
        Assert.That(
            () => ShardMap.GetOrCreateDefaultShared(virtualShardCount: 4, physicalShardCount: 8),
            Throws.ArgumentException);
    }

    [Test]
    public void CreateDefault_still_returns_distinct_instances_for_mutating_callers()
    {
        // The ?? fallback callsites use GetOrCreateDefaultShared; the
        // mutating callsite (TreeReshardGrain's empty-tree fast-path)
        // must still receive a fresh, writable map from CreateDefault so a
        // downstream SetShardMapAsync (which mutates Version) cannot
        // corrupt a cached instance.
        var a = ShardMap.CreateDefault(4096, 4);
        var b = ShardMap.CreateDefault(4096, 4);
        Assert.That(b, Is.Not.SameAs(a));
    }
}
