using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coordinator-side coverage for issue 4135. The leaf's
/// <c>CompactTombstonesAsync</c> is now bounded, so a tombstone-heavy leaf
/// legitimately returns having reaped only part of its condemned set. That is a
/// successful return, not a fault - which is precisely what makes it dangerous
/// to the drain: completing the shard calls
/// <c>ClearDirtyLeavesUpToAsync(advance)</c>, which removes every entry marked
/// at or below the pass watermark, so a partial leaf that was not re-marked
/// would be dropped from the dirty set with work still outstanding.
/// <para>
/// This is the same failure shape issue 2926 refused for a <em>throwing</em>
/// leaf ("improves every observable while degrading the thing being measured"),
/// reached by a different route: there the leaf announced itself with an
/// exception, here it returns cleanly. The remedy is the same mechanism -
/// re-mark the leaf strictly above the watermark before the walk moves past it,
/// and let the existing strictly-greater preservation rule retain it.
/// </para>
/// <para>
/// <b>Ordering is load-bearing, and asserting both calls happened is not
/// enough.</b> In the opposite order the drain would discard the very mark the
/// retain exists to preserve, and both assertions would still pass.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    /// <summary>
    /// Stubs a shard whose dirty-leaf snapshot names <paramref name="dirtyLeaves"/>,
    /// where the leaf at each index for which <paramref name="partialAtIndex"/>
    /// returns true reports a budget-truncated compaction pass - a successful
    /// return carrying <c>Completed=false</c>, not an exception.
    /// </summary>
    private static (IShardRootGrain ShardRoot, IBPlusLeafGrain[] Leaves) SetupShardWithPartialDirtyLeaves(
        IGrainFactory grainFactory,
        int shardIndex,
        HybridLogicalClock observedAdvance,
        Func<int, bool> partialAtIndex,
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

        // The dirty-set path names its leaves outright, so a fall-through to the
        // legacy chain walk must fail loudly rather than pass for the wrong reason.
        shardRoot.GetLeftmostLeafIdAsync()
            .Returns<GrainId?>(_ => throw new InvalidOperationException(
                "dirty-set fast path must not call GetLeftmostLeafIdAsync"));

        var leaves = new IBPlusLeafGrain[dirtyLeaves.Length];
        for (var i = 0; i < dirtyLeaves.Length; i++)
        {
            var leafMock = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(dirtyLeaves[i]).Returns(leafMock);
            leafMock.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(
                Task.FromResult(partialAtIndex(i)
                    ? LeafCompactionResult.Truncated(7)
                    : LeafCompactionResult.Complete(3)));
            leafMock.GetNextSiblingAsync()
                .Returns<GrainId?>(_ => throw new InvalidOperationException(
                    "dirty-set fast path must not call GetNextSiblingAsync"));
            leaves[i] = leafMock;
        }

        return (shardRoot, leaves);
    }

    [Test]
    public async Task Partially_compacted_leaf_has_its_dirty_mark_retained_before_the_drain_runs()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var partial = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var complete = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, leaves) = SetupShardWithPartialDirtyLeaves(
            grainFactory, 0, advance, i => i == 0, partial, complete);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        // The walk advances past the partial leaf in the same pass - a partial
        // result is a leaf-scoped condition and must not stall the shard, just
        // as a leaf fault no longer does (issue 2926).
        await leaves[0].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await leaves[1].Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        Received.InOrder(() =>
        {
            shardRoot.RetainDirtyLeafAsync(partial, advance);
            shardRoot.ClearDirtyLeavesUpToAsync(advance);
        });
    }

    [Test]
    public async Task Fully_compacted_leaf_is_not_re_marked_so_the_drain_can_actually_clear_it()
    {
        // The negative half of the pair. Re-marking unconditionally would retain
        // every leaf forever, so the dirty set would never shrink and the fast
        // path would degrade to a full re-walk on every pass - a quieter version
        // of the same "never drains" failure.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var first = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var second = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, _) = SetupShardWithPartialDirtyLeaves(
            grainFactory, 0, advance, _ => false, first, second);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        await shardRoot.DidNotReceive().RetainDirtyLeafAsync(Arg.Any<GrainId>(), Arg.Any<HybridLogicalClock>());
        await shardRoot.Received(1).ClearDirtyLeavesUpToAsync(advance);
        Assert.That(state.State.CurrentShardDirtyLeaves, Is.Null,
            "a dirty set whose every leaf completed must not be retained for the next pass");
    }

    [Test]
    public async Task Partial_leaf_whose_dirty_mark_cannot_be_retained_fails_the_batch_without_draining()
    {
        // A loud stall beats a quiet omission. If the mark cannot be raised
        // above the watermark there is no way to stop the drain discarding the
        // leaf, so the batch is failed rather than completed - the same refusal
        // the skip path makes, for the same reason. The fault is absorbed by the
        // shard-level retry policy in ProcessNextShardAsync, so what is
        // observable here is the drain NOT running, not an exception escaping.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var advance = HybridLogicalClock.Tick(default);
        var partial = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var (shardRoot, _) = SetupShardWithPartialDirtyLeaves(
            grainFactory, 0, advance, _ => true, partial);
        shardRoot.RetainDirtyLeafAsync(Arg.Any<GrainId>(), Arg.Any<HybridLogicalClock>())
            .Returns(_ => Task.FromException(
                new InvalidOperationException("shard root unavailable")));
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);
        await grain.ProcessNextShardAsync();

        // The drain is the thing being defended. It must not run, because
        // running it would discard the mark the retain failed to lift - leaving
        // a leaf with condemned entries dropped from the dirty set entirely.
        await shardRoot.DidNotReceive().ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardRetries, Is.EqualTo(1),
                "the batch failed, so the shard retry policy applies");
            Assert.That(state.State.NextShardIndex, Is.Zero,
                "and the coordinator stays on the shard rather than advancing past it");
        });
    }
}
