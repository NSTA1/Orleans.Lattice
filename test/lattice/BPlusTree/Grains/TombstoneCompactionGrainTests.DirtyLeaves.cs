using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the dirty-leaves compaction fast path. The
/// coordinator pulls a per-shard dirty-leaf snapshot from the shard
/// root before walking; a non-empty snapshot routes the pass through
/// the named leaves and HLC-gates the post-walk clear, while an empty
/// snapshot falls back to the legacy chain walk so an upgraded silo
/// with no accumulated signal still progresses.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    private static IShardRootGrain SetupShardWithDirtyLeaves(
        IGrainFactory grainFactory,
        int shardIndex,
        HybridLogicalClock observedAdvance,
        params GrainId[] dirtyLeaves)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIndex}")
            .Returns(shardRoot);

        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns(Task.FromResult(new DirtyLeavesSnapshot
            {
                DirtyLeaves = [.. dirtyLeaves],
                ObservedAdvance = observedAdvance,
            }));

        // Defensive: if anything ever asks for the leftmost leaf on a
        // dirty-set-stubbed shard the test should fail loudly rather
        // than fall back into the legacy chain walk - the coordinator
        // must consume the snapshot for these tests.
        shardRoot.GetLeftmostLeafIdAsync()
            .Returns<GrainId?>(_ => throw new InvalidOperationException(
                "dirty-set fast path must not call GetLeftmostLeafIdAsync"));

        foreach (var leafId in dirtyLeaves)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leafMock);
            leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(0));
            // The fast path indexes into the snapshot list; sibling
            // navigation must never be called on the dirty-set path.
            leafMock.GetNextSiblingAsync()
                .Returns<GrainId?>(_ => throw new InvalidOperationException(
                    "dirty-set fast path must not call GetNextSiblingAsync"));
        }

        return shardRoot;
    }

    [Test]
    public async Task DirtyLeaves_fast_path_visits_only_named_leaves_and_clears_with_watermark()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var dirty0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var dirty1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot = SetupShardWithDirtyLeaves(grainFactory, 0, advance, dirty0, dirty1);
        SetupShardWithLeaves(grainFactory, 1); // empty snapshot fallback for shard 1

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Single tick on shard 0 should visit both dirty leaves and
        // advance to shard 1 (the dirty list is exhausted).
        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(dirty0).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(dirty1).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shardRoot.Received(1).ClearDirtyLeavesUpToAsync(advance);
    }

    [Test]
    public async Task DirtyLeaves_empty_snapshot_falls_back_to_chain_walk()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        // Legacy chain walk visited the leftmost leaf and the shard
        // root's ClearDirtyLeavesUpToAsync was NOT called (no fast-path
        // watermark to drain).
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0");
        await shardRoot.DidNotReceive().ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>());
    }

    [Test]
    public async Task DirtyLeaves_fast_path_yields_within_shard_on_batch_boundary()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 2,
        };
        var (grain, state, _, grainFactory, _) = CreateGrain(options);

        var advance = HybridLogicalClock.Tick(default);
        var dirty0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var dirty1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var dirty2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var shardRoot = SetupShardWithDirtyLeaves(grainFactory, 0, advance, dirty0, dirty1, dirty2);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Tick 1: visits dirty0, dirty1 -> cursor parks at index 2 in
        // the persisted dirty-leaves snapshot.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
        Assert.That(state.State.CurrentShardDirtyIndex, Is.EqualTo(2));
        Assert.That(state.State.NextLeafKeyInShard, Is.Null,
            "the fast path resumes by list index, not by key");
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Not.Null);
        Assert.That(state.State.CurrentShardDirtyLeaves!.Length, Is.EqualTo(3));
        await grainFactory.GetGrain<IBPlusLeafGrain>(dirty2).DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shardRoot.DidNotReceive().ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>());

        // Tick 2: visits dirty2 -> shard advances, snapshot cleared,
        // watermark drained.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        Assert.That(state.State.CurrentShardDirtyIndex, Is.EqualTo(0));
        Assert.That(state.State.NextLeafKeyInShard, Is.Null);
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Null);
        await grainFactory.GetGrain<IBPlusLeafGrain>(dirty2).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await shardRoot.Received(1).ClearDirtyLeavesUpToAsync(advance);
    }

    [Test]
    public async Task DefaultCompactionShardTickInterval_is_500ms()
    {
        // Acceptance: the default tick is lowered to 500ms.
        Assert.That(LatticeOptions.DefaultCompactionShardTickInterval,
            Is.EqualTo(TimeSpan.FromMilliseconds(500)));
    }
}
