using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the handoff between one shard and the next, where the
/// dirty-leaves fast path meets the shard retry/skip policy.
/// <para>
/// <c>CurrentShardDirtyLeaves</c> and <c>CurrentShardDirtyAdvance</c> are
/// scoped to the shard that nominated them, so neither may survive the
/// coordinator leaving that shard. Carrying either forward makes the next
/// shard walk leaves it was never handed and drain its dirty set against a
/// watermark it never observed, which discards dirty-leaf signal without
/// compacting the leaves that raised it.
/// </para>
/// <para>
/// The precondition is narrow, and it is why the existing arms miss this. A
/// shard that fails on its <b>first</b> batch is already safe: that batch
/// fetched the snapshot itself, so the cursor captured before the call still
/// holds <see langword="null"/> and the failure handler's restore reverts
/// the fetch along with everything else. A list only outlives its shard once
/// a batch boundary has persisted it, so the leak needs a shard whose walk
/// spans more than one batch and then fails. Every existing retry/skip arm
/// fails at <c>GetDirtyLeavesSinceLastCompactionAsync</c> before any list
/// exists, and every existing fast-path arm completes its shard.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    /// <summary>
    /// A shard whose dirty list spans two batches and whose last leaf never
    /// compacts: the shape that leaves a persisted list behind when the
    /// shard is finally abandoned.
    /// </summary>
    private static (GrainId First, GrainId Second, GrainId Wedged, IShardRootGrain ShardRoot)
        SetupWedgedMultiBatchShard(IGrainFactory grainFactory, int shardIndex, HybridLogicalClock advance)
    {
        var first = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var second = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var wedged = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot = SetupShardWithDirtyLeaves(grainFactory, shardIndex, advance, first, second, wedged);

        // A leaf whose activation cannot complete inside the request
        // timeout fails every attempt, on whichever shard's behalf it is
        // called - which is what would make a leaked list self-propagating.
        grainFactory.GetGrain<IBPlusLeafGrain>(wedged)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns<int>(_ => throw new TimeoutException("leaf activation exceeded the request timeout"));

        return (first, second, wedged, shardRoot);
    }

    private static LatticeOptions TwoLeafBatches() => new()
    {
        TombstoneGracePeriod = TimeSpan.FromHours(24),
        CompactionLeafBatchSize = 2,
    };

    [Test]
    public async Task Skipping_a_shard_clears_the_shard_scoped_dirty_leaf_snapshot()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain(TwoLeafBatches());

        var advance0 = HybridLogicalClock.Tick(default);
        SetupWedgedMultiBatchShard(grainFactory, 0, advance0);
        SetupShardWithDirtyLeaves(grainFactory, 1, HybridLogicalClock.Tick(advance0),
            GrainId.Create("leaf", Guid.NewGuid().ToString()));

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        await grain.ProcessNextShardAsync(); // first batch parks mid-shard, persisting the list
        Assert.That(state.State.CurrentShardDirtyIndex, Is.EqualTo(2));
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Not.Null);

        await grain.ProcessNextShardAsync(); // the wedged leaf fails; retry the same shard
        Assert.That(state.State.ShardRetries, Is.EqualTo(1));
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Not.Null,
            "a retried shard resumes its own walk, so its snapshot survives");

        await grain.ProcessNextShardAsync(); // budget spent: leave the shard for good
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1), "the shard was skipped");
        Assert.Multiple(() =>
        {
            Assert.That(state.State.CurrentShardDirtyLeaves, Is.Null,
                "the abandoned shard's leaf list must not outlive the shard that nominated it");
            Assert.That(state.State.CurrentShardDirtyAdvance, Is.EqualTo(default(HybridLogicalClock)),
                "nor may its watermark, which is only meaningful against that shard's dirty set");
        });
    }

    [Test]
    public async Task A_skipped_shard_does_not_hand_its_dirty_leaf_list_to_the_next_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain(TwoLeafBatches());

        var advance0 = HybridLogicalClock.Tick(default);
        var advance1 = HybridLogicalClock.Tick(advance0);
        var (first, _, _, _) = SetupWedgedMultiBatchShard(grainFactory, 0, advance0);
        var ownLeaf = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot1 = SetupShardWithDirtyLeaves(grainFactory, 1, advance1, ownLeaf);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        await grain.ProcessNextShardAsync(); // shard 0, batch 1: first, second
        await grain.ProcessNextShardAsync(); // shard 0, batch 2: wedged fails, retry
        await grain.ProcessNextShardAsync(); // shard 0 abandoned, advance to shard 1
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        await grain.ProcessNextShardAsync(); // shard 1's own turn

        await shardRoot1.Received(1).GetDirtyLeavesSinceLastCompactionAsync();
        await grainFactory.GetGrain<IBPlusLeafGrain>(ownLeaf).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(first).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shardRoot1.Received(1).ClearDirtyLeavesUpToAsync(advance1);
        await shardRoot1.DidNotReceive().ClearDirtyLeavesUpToAsync(advance0);
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2),
            "shard 1 completed its own walk rather than re-walking shard 0's leaves");
    }

    [Test]
    public async Task A_retried_shard_resumes_its_own_list_rather_than_refetching_it()
    {
        // The positive control for the two arms above: clearing the
        // snapshot when the coordinator LEAVES a shard must not become
        // clearing it whenever a leaf fails. A retry stays on the same
        // shard, so re-fetching would restart the walk and re-compact
        // leaves the previous batch already finished.
        var (grain, state, _, grainFactory, _) = CreateGrain(TwoLeafBatches());

        var advance0 = HybridLogicalClock.Tick(default);
        var (first, _, _, shardRoot0) = SetupWedgedMultiBatchShard(grainFactory, 0, advance0);
        SetupShardWithDirtyLeaves(grainFactory, 1, HybridLogicalClock.Tick(advance0),
            GrainId.Create("leaf", Guid.NewGuid().ToString()));

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        await grain.ProcessNextShardAsync(); // compacts first, second; parks at index 2
        Assert.That(state.State.CurrentShardDirtyIndex, Is.EqualTo(2));

        await grain.ProcessNextShardAsync(); // the wedged leaf fails; retry the shard

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardRetries, Is.EqualTo(1));
            Assert.That(state.State.NextShardIndex, Is.Zero);
            Assert.That(state.State.CurrentShardDirtyLeaves, Is.Not.Null);
            Assert.That(state.State.CurrentShardDirtyLeaves!.Length, Is.EqualTo(3));
            Assert.That(state.State.CurrentShardDirtyIndex, Is.EqualTo(2),
                "the retry resumes at the wedged leaf rather than restarting the shard");
        });
        await shardRoot0.Received(1).GetDirtyLeavesSinceLastCompactionAsync();
        await grainFactory.GetGrain<IBPlusLeafGrain>(first).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }
}
