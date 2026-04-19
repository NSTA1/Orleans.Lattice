using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for audit bug #1: <see cref="TombstoneCompactionGrain"/> must
/// (a) resolve the tree alias through the registry, so compaction targets the physical
/// tree id actually storing the shards, and (b) iterate the distinct physical shard
/// indices recorded in the current <see cref="ShardMap"/> rather than
/// <c>0..options.ShardCount</c>. Otherwise compaction silently skips shards created
/// by adaptive splits (slots above the initial count) and targets the wrong grain
/// key for aliased trees.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task Compaction_resolves_alias_and_targets_physical_tree_id()
    {
        const string physicalTreeId = "physical-tree";
        var (grain, state, _, grainFactory, _) = CreateGrain();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(physicalTreeId);
        registry.GetShardMapAsync(TreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, 2)));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Returns(registry);

        var physicalShardRoot = Substitute.For<IShardRootGrain>();
        physicalShardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/0")
            .Returns(physicalShardRoot);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        _ = physicalShardRoot.Received().GetLeftmostLeafIdAsync();

        // And the grain must never have asked for the logical-keyed shard root.
        grainFactory.DidNotReceive().GetGrain<IShardRootGrain>($"{TreeId}/0");
    }

    [Test]
    public async Task Compaction_iterates_physical_shards_from_ShardMap_not_option_count()
    {
        // Simulate a tree that has been adaptively split so physical shard
        // indices are {0, 5} — not contiguous and not bounded by options.ShardCount.
        var customMap = new ShardMap
        {
            Slots = [0, 0, 0, 0, 5, 5, 5, 5],
            Version = 3
        };

        var (grain, state, _, grainFactory, _) = CreateGrain();

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(customMap));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Returns(registry);

        var shard0 = Substitute.For<IShardRootGrain>();
        shard0.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        var shard5 = Substitute.For<IShardRootGrain>();
        shard5.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/5").Returns(shard5);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Two physical shards — must tick twice to cover {0, 5} before completing.
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync();
        await grain.ProcessNextShardAsync(); // triggers completion

        await shard0.Received().GetLeftmostLeafIdAsync();
        await shard5.Received().GetLeftmostLeafIdAsync();
        Assert.That(state.State.InProgress, Is.False,
            "Pass must complete after covering every physical shard in the map.");
    }
}
