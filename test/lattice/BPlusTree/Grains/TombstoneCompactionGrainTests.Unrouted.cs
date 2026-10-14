using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TombstoneCompactionGrainTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task DirtySet_rowless_leaf_is_discarded_only_when_proven_unrouted(bool unrouted)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var missing = GrainId.Create("leaf", "missing");
        var live = GrainId.Create("leaf", "live");
        var (shard, leaves) = SetupShardWithDirtyLeavesWhere(factory, 0, advance, _ => false, missing, live);
        leaves[0].CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns(_ => Task.FromException<LeafCompactionResult>(
                new LeafStateRowLostException(missing.ToString(), TreeId, "no row", null)));
        shard.DiscardUnroutedDirtyLeafAsync(missing).Returns(unrouted);
        SetupShardWithLeaves(factory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await shard.Received(1).DiscardUnroutedDirtyLeafAsync(missing);
        if (unrouted)
            await shard.DidNotReceive().RetainDirtyLeafAsync(missing, Arg.Any<HybridLogicalClock>());
        else
            await shard.Received(1).RetainDirtyLeafAsync(missing, advance);
        await leaves[1].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shard.Received(1).ClearDirtyLeavesUpToAsync(advance);
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
    }

    [Test]
    public async Task DirtySet_failed_routing_proof_retains_the_rowless_leaf()
    {
        var (grain, _, _, factory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var missing = GrainId.Create("leaf", "missing");
        var (shard, leaves) = SetupShardWithDirtyLeavesWhere(factory, 0, advance, _ => false, missing);
        leaves[0].CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns(_ => Task.FromException<LeafCompactionResult>(
                new LeafStateRowLostException(missing.ToString(), TreeId, "no row", null)));
        shard.DiscardUnroutedDirtyLeafAsync(missing)
            .Returns(_ => Task.FromException<bool>(new InvalidOperationException("routing unavailable")));
        SetupShardWithLeaves(factory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await shard.Received(1).RetainDirtyLeafAsync(missing, advance);
        await shard.Received(1).ClearDirtyLeavesUpToAsync(advance);
    }
}
