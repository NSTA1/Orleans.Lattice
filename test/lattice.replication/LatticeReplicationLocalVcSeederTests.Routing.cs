using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression coverage for which shards <see cref="LatticeReplicationLocalVcSeeder"/>
/// walks: the ones the tree's live routing reaches, not
/// <c>{treeName}/0..ShardCount-1</c>. A restore puts the tree behind an alias
/// (so the pinned range names the retired pre-restore copy) and an adaptive
/// split moves slots to a shard above the pinned count (so the pinned range
/// misses it).
/// </summary>
public partial class LatticeReplicationLocalVcSeederTests
{
    private const string RestoredPhysicalTree = "restored-tree-shadow-1";

    private static IShardRootGrain ShardWithOneLeaf(IGrainFactory factory, string shardKey, LwwEntry entry)
    {
        var leafId = GrainId.Create("test-leaf", shardKey);
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetLiveRawEntriesAsync().Returns(Task.FromResult(new List<LwwEntry> { entry }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var shard = Substitute.For<IShardRootGrain>();
        shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        factory.GetGrain<IShardRootGrain>(shardKey).Returns(shard);
        return shard;
    }

    [Test]
    public async Task SeedFromTreeAsync_walks_the_routed_physical_shards_including_a_split_target()
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var hwmGrain = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmGrain);

        // The pinned count is 1, but the routing map reaches the restored
        // physical copy's shard 0 and the split target shard 5 above the pin.
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardIndicesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<int>>(Enumerable.Range(0, 1).ToArray()));
        shardCounts.GetShardRootKeysAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(
                [$"{RestoredPhysicalTree}/0", $"{RestoredPhysicalTree}/5"]));

        ShardWithOneLeaf(factory, $"{RestoredPhysicalTree}/0", Entry("k0", Vector((OriginA, Hlc(10)))));
        ShardWithOneLeaf(factory, $"{RestoredPhysicalTree}/5", Entry("k5", Vector((OriginB, Hlc(20)))));

        // The retired pre-restore copy still answers under the logical id and
        // carries a frontier the restore replaced; it must not be walked.
        var retired = ShardWithOneLeaf(factory, $"{Tree}/0", Entry("old", Vector((OriginC, Hlc(99)))));

        var seeder = new LatticeReplicationLocalVcSeeder(factory, shardCounts, resolver);
        var report = await seeder.SeedFromTreeAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.SeedApplied, Is.True);
            Assert.That(report.EntriesScanned, Is.EqualTo(2));
            Assert.That(report.Frontier!.Entries.Keys, Is.EquivalentTo(new[] { OriginA, OriginB }));
            Assert.That(report.Frontier.Entries[OriginB], Is.EqualTo(Hlc(20)));
        });
        await retired.DidNotReceive().GetLeftmostLeafIdAsync();
        await shardCounts.DidNotReceive().GetShardIndicesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
