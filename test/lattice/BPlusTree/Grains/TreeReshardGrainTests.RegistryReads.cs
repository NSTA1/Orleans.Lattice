using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The recurring reshard ticks read the shard map and the physical tree id
/// from one registry entry, rather than paying a separate
/// <see cref="ILatticeRegistry.GetShardMapAsync"/> and
/// <see cref="ILatticeRegistry.ResolveAsync"/> round trip on every tick.
/// </summary>
public partial class TreeReshardGrainTests
{
    [Test]
    public async Task MigrateAsync_grow_tick_reads_map_and_physical_tree_from_one_registry_entry()
    {
        var (grain, state, grainFactory, registry) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 2,
            existingMap: ShardMap.CreateDefault(16, 2), maxConcurrentMigrations: 4);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;
        var shard = Substitute.For<IShardRootGrain>();
        shard.IsSplittingAsync().Returns(false);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        grainFactory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>())
            .Returns(Substitute.For<ITreeShardSplitGrain>());

        await grain.MigrateAsync();

        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
        await registry.Received().GetEntryAsync(TreeId);
    }

    [Test]
    public async Task MigrateAsync_shrink_tick_reads_map_and_physical_tree_from_one_registry_entry()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);
        var registry = h.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        await h.Grain.MigrateAsync();

        await h.Folds[1].Received(1).StartAsync(0);
        await registry.DidNotReceive().GetShardMapAsync(Arg.Any<string>());
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
        await registry.Received().GetEntryAsync(TreeId);
    }
}
