using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4234, case 2: on a physical copy a logical
/// tree's alias targets, a split the tree makes after the alias was set is
/// recorded on the logical tree's registry entry, so the lifecycle walk must
/// fold that record in with the copy's own.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private const string OwnerTreeId = "logical-owner";

    /// <summary>
    /// Records <see cref="TreeId"/> as a copy derived from
    /// <see cref="OwnerTreeId"/>, whose own entry carries a split allocated at
    /// <see cref="SplitShardIndex"/> and whose alias targets the copy only when
    /// <paramref name="ownerAliasesThisCopy"/> is set.
    /// </summary>
    private static void UseAliasedCopyTopology(IGrainFactory grainFactory, bool ownerAliasesThisCopy)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
                DerivedFrom = OwnerTreeId,
            }));

        var slots = (int[])ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount).Slots.Clone();
        slots[0] = SplitShardIndex;
        registry.GetEntryAsync(OwnerTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                ShardCount = ShardCount,
                PhysicalTreeId = ownerAliasesThisCopy ? TreeId : "some-other-copy",
                ShardMap = new ShardMap { Slots = slots, Version = 3 },
                NextShardIndex = SplitShardIndex,
            }));

        for (var i = ShardCount; i <= SplitShardIndex; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.UnmarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
            shardRoot.ReseedNodeBindingsAsync().Returns(Task.CompletedTask);
        }
    }

    [Test]
    public async Task ResolveAllocatedShardCount_folds_in_the_owning_logical_trees_split_record_while_it_aliases_this_copy()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseAliasedCopyTopology(grainFactory, ownerAliasesThisCopy: true);

        Assert.That(await grain.ResolveAllocatedShardCountAsync(), Is.EqualTo(SplitShardIndex + 1));
    }

    [Test]
    public async Task ResolveAllocatedShardCount_ignores_the_owner_once_its_alias_targets_another_copy()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseAliasedCopyTopology(grainFactory, ownerAliasesThisCopy: false);

        Assert.That(await grain.ResolveAllocatedShardCountAsync(), Is.EqualTo(ShardCount));
    }

    [Test]
    public async Task DeleteDelegated_marks_a_shard_split_onto_the_copy_after_the_alias()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        UseAliasedCopyTopology(grainFactory, ownerAliasesThisCopy: true);

        await grain.DeleteDelegatedAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{SplitShardIndex}").Received(1).MarkDeletedAsync();
    }
}
