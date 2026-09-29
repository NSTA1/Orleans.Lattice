using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class RoutedShardIndicesTests
{
    private static ShardMap MapRouting(int shardCount, params (int Slot, int Shard)[] moves)
    {
        var slots = (int[])ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, shardCount).Slots.Clone();
        foreach (var (slot, shard) in moves) slots[slot] = shard;
        return new ShardMap { Slots = slots, Version = 1 };
    }

    [Test]
    public void Resolve_is_the_pinned_range_without_a_map()
    {
        Assert.That(RoutedShardIndices.Resolve(4, null), Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public void Resolve_adds_a_shard_a_split_allocated_above_the_pinned_count()
    {
        var map = MapRouting(2, (0, 5));

        Assert.That(RoutedShardIndices.Resolve(2, map), Is.EqualTo(new[] { 0, 1, 5 }));
    }

    [Test]
    public void Resolve_keeps_a_pinned_shard_the_map_no_longer_routes_to()
    {
        // Every slot of shard 1 reassigned to shard 0, as a consolidation leaves it.
        var map = new ShardMap { Slots = new int[LatticeConstants.DefaultVirtualShardCount], Version = 3 };

        Assert.That(RoutedShardIndices.Resolve(2, map), Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public void Resolve_rejects_a_negative_shard_count()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RoutedShardIndices.Resolve(-1, null));
    }

    [Test]
    public void OrContiguous_returns_the_persisted_set()
    {
        int[] persisted = [0, 1, 7];

        Assert.That(RoutedShardIndices.OrContiguous(persisted, 2), Is.SameAs(persisted));
    }

    [Test]
    public void OrContiguous_falls_back_to_the_pinned_range_for_legacy_state()
    {
        Assert.That(RoutedShardIndices.OrContiguous(null, 3), Is.EqualTo(new[] { 0, 1, 2 }));
    }
}
