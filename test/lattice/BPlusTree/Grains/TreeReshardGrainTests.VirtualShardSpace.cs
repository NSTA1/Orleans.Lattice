using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the reshard bound and the empty-tree rebuild both
/// honouring the tree's own virtual slot space. An installed app can pin a map
/// over fewer than the default 4096 slots, but the reshard validated its target
/// against 4096 and rebuilt an empty tree's map over 4096 slots. On a populated
/// tree a target above the slot count therefore passed validation and was never
/// reached - a split needs a source owning at least two slots - so the reshard
/// stayed in progress indefinitely; on an empty tree the declared slot space was
/// silently discarded.
/// </summary>
public partial class TreeReshardGrainTests
{
    [Test]
    public void ReshardAsync_throws_when_target_exceeds_the_trees_own_virtual_shard_count()
    {
        // The fixture map has 16 slots, far below the 4096 default.
        var (grain, state, _, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.ReshardAsync(17));
        Assert.That(state.State.InProgress, Is.False,
            "an unreachable target must be refused, not latched as an in-progress reshard");
    }

    [Test]
    public async Task ReshardAsync_accepts_a_target_equal_to_the_trees_own_virtual_shard_count()
    {
        var (grain, state, _, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);

        await grain.ReshardAsync(16);

        AssertTookNormalCoordinatorPath(state, 16);
    }

    [Test]
    public void ReshardAsync_keeps_the_4096_ceiling_on_a_tree_with_a_larger_virtual_shard_space()
    {
        var (grain, _, _, _) = CreateGrain(virtualShardCount: 8192, physicalShardCount: 2);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.ReshardAsync(LatticeConstants.DefaultVirtualShardCount + 1));
    }

    [Test]
    public async Task ReshardAsync_empty_tree_fast_path_keeps_the_trees_own_virtual_shard_count()
    {
        var (grain, _, grainFactory, registry) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);
        StubEveryShard(grainFactory, s => s.AnyBoundedAsync(Arg.Any<string?>())
            .Returns(Task.FromResult(new ShardAnyPage { Found = false })));

        await grain.ReshardAsync(4);

        await registry.Received(1).SetShardMapAsync(
            TreeId,
            Arg.Is<ShardMap>(m => m.VirtualShardCount == 16 && m.GetPhysicalShardIndices().Count == 4));
    }
}
