using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4234, case 1: the empty-tree fast path
/// rewrites the pin and the shard map, and must also raise the split-allocation
/// high-water mark to the highest shard index the tree had, or tree deletion
/// stops walking the shards a downward re-pin dropped.
/// </summary>
public partial class TreeReshardGrainTests
{
    [Test]
    public async Task Empty_tree_downward_repin_raises_the_high_water_mark_to_the_dropped_shards()
    {
        var (grain, _, grainFactory, registry) = CreateGrain(physicalShardCount: 4);
        SetupEmptyShards(grainFactory);

        await grain.ReshardAsync(2);

        await registry.Received(1).UpdateAsync(TreeId,
            Arg.Is<TreeRegistryEntry>(e => e.ShardCount == 2 && e.NextShardIndex == 3));
    }

    [Test]
    public async Task Empty_tree_repin_keeps_a_higher_existing_high_water_mark()
    {
        var (grain, _, grainFactory, registry) = CreateGrain(physicalShardCount: 4);
        SetupEmptyShards(grainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4, NextShardIndex = 7 }));

        await grain.ReshardAsync(2);

        await registry.Received(1).UpdateAsync(TreeId,
            Arg.Is<TreeRegistryEntry>(e => e.ShardCount == 2 && e.NextShardIndex == 7));
    }

    [Test]
    public async Task Empty_tree_repin_covers_a_map_index_above_the_pin()
    {
        var map = ShardMap.CreateDefault(16, 2);
        var slots = (int[])map.Slots.Clone();
        slots[0] = 5;
        var (grain, _, grainFactory, registry) = CreateGrain(
            physicalShardCount: 2, existingMap: new ShardMap { Slots = slots, Version = 2 });
        SetupEmptyShards(grainFactory);

        await grain.ReshardAsync(2);

        await registry.Received(1).UpdateAsync(TreeId,
            Arg.Is<TreeRegistryEntry>(e => e.ShardCount == 2 && e.NextShardIndex == 5));
    }
}
