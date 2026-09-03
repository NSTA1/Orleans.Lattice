using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1970: the cross-batch resume cursor
/// (<c>NextLeafIdInShard</c>) is a leaf <see cref="GrainId"/>, and Orleans
/// grains are virtual - so a cursor naming a leaf whose state no longer exists
/// activates a fresh, EMPTY grain rather than failing.
/// <para>
/// That empty grain reports a null sibling, so an unvalidated resume walks zero
/// leaves, concludes it has reached the end of the shard, and reports
/// <c>done=true</c> with the shard's remainder never compacted. The dangerous
/// part is that this is indistinguishable from a clean completion: no
/// exception, no metric, no log line - it simply under-compacts forever.
/// </para>
/// <para>
/// No current code path reclaims a leaf: the sibling chain is grow-only
/// (splits and bulk-load graft insert; nothing unlinks), and leaf state is
/// cleared only by <c>ShardRootGrain.PurgeAsync</c>, which by contract runs
/// against an already-offline tree. These tests therefore pin a property that
/// is currently unreachable, so that a future change which does reclaim leaves
/// fails here loudly instead of silently under-compacting in production.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    /// <summary>
    /// Builds a shard leaf chain in which one leaf is <b>reclaimed</b>: Orleans
    /// hands back a fresh activation for that identity whose state is empty, so
    /// it reports no tree id and no sibling.
    /// <para>
    /// The reclaimed leaf is stubbed on first registration rather than by
    /// re-stubbing a live one, so there is exactly one return configuration per
    /// call and the test cannot be confounded by substitute override semantics.
    /// </para>
    /// </summary>
    private static void SetupShardWithReclaimedLeaf(
        IGrainFactory grainFactory,
        int shardIndex,
        GrainId reclaimedLeaf,
        params GrainId[] leafIds)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIndex}").Returns(shardRoot);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns(Task.FromResult(new DirtyLeavesSnapshot
            {
                DirtyLeaves = [],
                ObservedAdvance = default,
            }));
        shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));

        for (int i = 0; i < leafIds.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leafMock);
            leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(0));

            var isReclaimed = leafIds[i] == reclaimedLeaf;
            leafMock.GetTreeIdAsync().Returns(
                Task.FromResult<string?>(isReclaimed ? null : TreeId));

            var nextId = i + 1 < leafIds.Length ? (GrainId?)leafIds[i + 1] : null;
            leafMock.GetNextSiblingAsync().Returns(
                Task.FromResult(isReclaimed ? null : nextId));
        }
    }

    /// <summary>
    /// The core property: a resume cursor pointing at a reclaimed leaf must not
    /// silently end the shard. Falling back to a full re-walk is safe because
    /// per-leaf compaction is idempotent - the same trade the dirty-set resume
    /// path already makes for a cursor it cannot locate.
    /// </summary>
    [Test]
    public async Task ProcessNextShard_resuming_from_a_reclaimed_leaf_still_compacts_the_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithReclaimedLeaf(grainFactory, 0, reclaimedLeaf: leaf1, leaf0, leaf1, leaf2);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // A previous batch stopped at leaf1, which has since been reclaimed.
        state.State.NextLeafIdInShard = leaf1.ToString();

        await grain.ProcessNextShardAsync();

        // The walk must not have accepted the dead cursor and stopped. Falling
        // back to the leftmost leaf re-walks the chain, so the surviving leaves
        // are compacted rather than skipped.
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Received()
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    /// <summary>
    /// A live leaf must still be resumed from directly. Without this the fix
    /// would "pass" by simply restarting every batch, throwing away the cursor
    /// and turning a bounded walk into repeated full re-walks.
    /// </summary>
    [Test]
    public async Task ProcessNextShard_resuming_from_a_live_leaf_does_not_rewalk_from_the_start()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1, leaf2);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        state.State.NextLeafIdInShard = leaf1.ToString();

        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).DidNotReceive()
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    /// <summary>
    /// A cursor probe that throws is a reachable-but-faulting leaf, not a
    /// reclaimed one - an empty virtual activation returns null cleanly. It
    /// must therefore be treated as live and left to the walk's existing
    /// per-leaf retry and skip handling, so a transient blip does not restart
    /// the whole shard on every batch.
    /// </summary>
    [Test]
    public async Task ProcessNextShard_treats_a_faulting_cursor_probe_as_live()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1);

        var faulting = grainFactory.GetGrain<IBPlusLeafGrain>(leaf1);
        faulting.GetTreeIdAsync().Throws(new InvalidOperationException("transient"));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        state.State.NextLeafIdInShard = leaf1.ToString();

        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).DidNotReceive()
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await faulting.Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }
}
