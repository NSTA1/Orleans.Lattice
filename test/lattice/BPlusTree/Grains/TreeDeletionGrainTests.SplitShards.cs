using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for tree deletion, recovery and purge on a tree whose
/// adaptive shard splits allocated physical shard indices above the pinned
/// <c>ShardCount</c>. The lifecycle walks used to stop at
/// <c>ShardCount - 1</c>, so a split-added shard kept serving reads and
/// writes after <c>DeleteTreeAsync</c> and kept its state after a purge.
/// </summary>
public partial class TreeDeletionGrainTests
{
    /// <summary>
    /// Physical index an adaptive split allocated for the tree: above the
    /// pinned <see cref="ShardCount"/> of 2 and reachable through the map.
    /// </summary>
    private const int SplitShardIndex = 3;

    /// <summary>
    /// Re-points the registry entry at a split topology - the pinned
    /// <c>ShardCount</c> is unchanged, the shard map routes a slot to
    /// <see cref="SplitShardIndex"/>, and the split allocation high-water mark
    /// records it - and stubs the shard roots the split introduced (index 2 is
    /// the target of an abandoned split, reachable only through the contiguous
    /// range up to the highest allocated index).
    /// </summary>
    private static void UseSplitTopology(IGrainFactory grainFactory, bool routeToSplitShard = true, int? nextShardIndex = SplitShardIndex)
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount);
        var slots = (int[])map.Slots.Clone();
        if (routeToSplitShard)
        {
            slots[0] = SplitShardIndex;
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
                ShardMap = new ShardMap { Slots = slots, Version = 2 },
                NextShardIndex = nextShardIndex,
            }));

        for (var i = ShardCount; i <= SplitShardIndex; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.UnmarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
            shardRoot.ReseedNodeBindingsAsync(Arg.Any<int>()).Returns(Task.FromResult(-1));
        }
    }

    [Test]
    public async Task DeleteTree_marks_split_allocated_shards_above_the_pinned_shard_count()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory);

        await grain.DeleteTreeAsync();

        for (var i = 0; i <= SplitShardIndex; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Recover_unmarks_and_reseeds_split_allocated_shards()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory);
        await grain.DeleteTreeAsync();

        await grain.RecoverAsync();

        for (var i = 0; i <= SplitShardIndex; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).UnmarkDeletedAsync();
            await shard.Received(1).ReseedNodeBindingsAsync(0);
        }
    }

    [Test]
    public async Task PurgeNow_purges_split_allocated_shards()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory);
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        for (var i = 0; i <= SplitShardIndex; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).PurgeAsync();
        }
        Assert.That(state.State.PurgeComplete, Is.True);
    }

    [Test]
    public async Task ProcessNextShard_walks_every_split_allocated_shard_before_completing()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory);
        state.State.IsDeleted = true;
        state.State.PurgeInProgress = true;
        state.State.NextShardIndex = 0;

        for (var i = 0; i <= SplitShardIndex; i++)
        {
            await grain.ProcessNextShardAsync();
            Assert.That(state.State.PurgeComplete, Is.False,
                $"purge completed after shard {i} with shard {SplitShardIndex} still owed");
        }

        await grain.ProcessNextShardAsync();

        Assert.That(state.State.PurgeComplete, Is.True);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{SplitShardIndex}").Received(1).PurgeAsync();
    }

    [Test]
    public async Task ResolveAllocatedShardCount_covers_a_map_index_above_an_absent_high_water_mark()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory, routeToSplitShard: true, nextShardIndex: null);

        Assert.That(await grain.ResolveAllocatedShardCountAsync(), Is.EqualTo(SplitShardIndex + 1));
    }

    [Test]
    public async Task ResolveAllocatedShardCount_covers_a_retired_shard_recorded_only_by_the_high_water_mark()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseSplitTopology(grainFactory, routeToSplitShard: false, nextShardIndex: SplitShardIndex);

        Assert.That(await grain.ResolveAllocatedShardCountAsync(), Is.EqualTo(SplitShardIndex + 1));
    }

    [Test]
    public async Task ResolveAllocatedShardCount_is_the_pinned_shard_count_for_an_unsplit_tree()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.That(await grain.ResolveAllocatedShardCountAsync(), Is.EqualTo(ShardCount));
    }
}
