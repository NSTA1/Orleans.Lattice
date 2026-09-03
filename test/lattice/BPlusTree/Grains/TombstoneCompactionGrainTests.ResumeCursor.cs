using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issues 1970 and 1973: the compactor's cross-batch
/// resume position must be a <b>key</b>, not a leaf <see cref="GrainId"/>.
/// <para>
/// Orleans grains are virtual, so a cursor naming a leaf whose state no longer
/// exists activates a fresh, EMPTY grain rather than failing. That empty grain
/// reports a null sibling, so a walk resumed from an id walks zero leaves,
/// concludes it has reached the end of the shard, and reports <c>done=true</c>
/// with the shard's remainder never compacted. The dangerous part is that this
/// is indistinguishable from a clean completion: no exception, no metric, no
/// log line - it simply under-compacts forever.
/// </para>
/// <para>
/// Issue 1970 defended against that with a liveness probe on the id. Issue 1973
/// removes the failure mode instead: the cursor is now a key, which the shard
/// root re-descends onto whichever leaf currently owns it. These tests pin the
/// resulting properties - that a resumed walk lands where the key says rather
/// than where an id used to point, that it does not re-walk from the start, and
/// that a structural change to the chain between two turns still leaves the
/// shard fully compacted.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task ProcessNextShard_resuming_from_a_key_cursor_does_not_rewalk_from_the_start()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1, leaf2);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        state.State.NextLeafKeyInShard = LeafResumeKey(1);

        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).DidNotReceive()
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    /// <summary>
    /// The property the key cursor exists for. The leaf a previous batch parked
    /// before is reclaimed between turns, so its identity now activates empty
    /// with a null sibling. Because the resume position is a key, the shard root
    /// re-descends it onto the leaf that now owns that key and the walk
    /// continues - where an id cursor would have handed the walk the dead
    /// activation and ended the shard silently.
    /// </summary>
    [Test]
    public async Task ProcessNextShard_resuming_after_the_cursor_leaf_is_reclaimed_still_compacts_the_rest_of_the_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var reclaimed = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var replacement = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());

        SetupShardWithLeaves(grainFactory, 0, leaf0, replacement, leaf2);

        // The identity a previous batch would have persisted as an id cursor is
        // now an empty virtual activation: no tree id, no sibling. The key
        // cursor never consults it, which is the whole point.
        var dead = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(reclaimed).Returns(dead);
        dead.GetTreeIdAsync().Returns(Task.FromResult<string?>(null));
        dead.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        dead.GetKeyRangeAsync().Returns(Task.FromResult(default(LeafKeyRange)));

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        state.State.NextLeafKeyInShard = LeafResumeKey(1);

        await grain.ProcessNextShardAsync();

        await dead.DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(replacement).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());

        Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
            "the shard must be reported complete only after its remaining leaves were visited");
        Assert.That(state.State.NextLeafKeyInShard, Is.Null);
    }

    /// <summary>
    /// A leaf-id cursor persisted by an older build must not be trusted as a
    /// key. It is discarded and the shard restarts from its leftmost leaf, which
    /// costs a re-walk and nothing else because per-leaf compaction is
    /// idempotent (issue 1973).
    /// </summary>
    [Test]
    public async Task ProcessNextShard_discards_a_legacy_leaf_id_cursor_and_restarts_the_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // State written by a build that persisted the cursor as a leaf id.
        state.State.NextLeafIdInShard = leaf1.ToString();
        state.State.NextLeafKeyInShard = null;

        await grain.ProcessNextShardAsync();

        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1)
            .CompactTombstonesAsync(Arg.Any<TimeSpan>());
        Assert.That(state.State.NextLeafIdInShard, Is.Null,
            "the superseded leaf-id cursor must be cleared so it cannot be re-read");
    }
}
