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
    public void VirtualShardCount_reflects_slot_array_length()
    {
        var map = new ShardMap { Slots = new int[16] };
        Assert.That(map.VirtualShardCount, Is.EqualTo(16));
    }
}
