using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the shard-root dirty-leaf tracking API exercised by
/// the compaction coordinator's fast path.
/// </summary>
[TestFixture]
public class ShardRootGrainDirtyLeavesTests
{
    private const string ShardKey = "dirty-tree/0";

    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State, IBPlusLeafGrain Leaf, GrainId LeafId)
        CreateGrain(FakePersistentState<ShardRootState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        state ??= new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "dirty-tree-leaf-0");
        state.State.RootNodeId ??= leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        leaf.DeleteTrackedAsync(Arg.Any<string>()).Returns(Task.FromResult(new LeafDeleteResult { Deleted = true }));
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 0, PastRange = true }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, state, leaf, leafId);
    }

    [Test]
    public async Task GetDirtyLeavesSinceLastCompaction_returns_empty_snapshot_on_fresh_grain()
    {
        var (grain, _, _, _) = CreateGrain();

        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();

        Assert.That(snapshot.DirtyLeaves, Is.Empty);
        Assert.That(snapshot.ObservedAdvance, Is.EqualTo(default(HybridLogicalClock)));
    }

    [Test]
    public async Task DeleteAsync_marks_routed_leaf_dirty()
    {
        var (grain, state, _, leafId) = CreateGrain();

        await grain.DeleteAsync("k1");

        Assert.That(state.State.DirtyLeavesSinceLastCompaction.ContainsKey(leafId.ToString()), Is.True);

        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();
        Assert.That(snapshot.DirtyLeaves, Has.Count.EqualTo(1));
        Assert.That(snapshot.DirtyLeaves[0], Is.EqualTo(leafId));
        Assert.That(snapshot.ObservedAdvance, Is.GreaterThan(default(HybridLogicalClock)));
    }

    [Test]
    public async Task DeleteAsync_does_not_persist_synchronously_under_coalescing()
    {
        // U9h-B: routed Deletes mutate the in-memory dirty-leaf dictionary
        // and arm a coalescing flush timer; the storage write is performed
        // off-path. In the test harness the grain-runtime timer cannot
        // register against the substituted IGrainContext, so the helper
        // logs and continues - the write count must remain zero across
        // repeated Deletes within the dirty window, matching the
        // production "one WriteStateAsync per coalescing window" contract.
        var (grain, state, _, _) = CreateGrain();

        var writesBefore = state.WriteCount;
        await grain.DeleteAsync("k1");
        await grain.DeleteAsync("k2");
        await grain.DeleteAsync("k3");

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task DeleteAsync_dedups_repeated_marks_within_window()
    {
        // U9h-B post-coalescing: the original "one-write-per-distinct-leaf-
        // per-window" dedup is now subsumed by the stronger "no writes from
        // DeleteAsync at all" guarantee. Repeated Deletes to the same leaf
        // (and to different leaves) must still leave WriteCount unchanged
        // because the flush is deferred to the timer / drain / deactivate.
        var (grain, state, _, _) = CreateGrain();

        await grain.DeleteAsync("k1");
        var writesAfterFirst = state.WriteCount;

        await grain.DeleteAsync("k2");
        await grain.DeleteAsync("k3");

        Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst));
    }

    [Test]
    public async Task ClearDirtyLeavesUpToAsync_drops_entries_at_or_before_watermark()
    {
        var (grain, state, _, leafId) = CreateGrain();

        await grain.DeleteAsync("k1");
        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();

        await grain.ClearDirtyLeavesUpToAsync(snapshot.ObservedAdvance);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Is.Empty);
        Assert.That(state.State.LastDirtyAdvance, Is.EqualTo(snapshot.ObservedAdvance));
        // Marking the leaf again post-clear must persist a fresh entry.
        await grain.DeleteAsync("k4");
        Assert.That(state.State.DirtyLeavesSinceLastCompaction.ContainsKey(leafId.ToString()), Is.True);
    }

    [Test]
    public async Task ClearDirtyLeavesUpToAsync_preserves_entries_marked_after_watermark()
    {
        var (grain, state, _, _) = CreateGrain();

        await grain.DeleteAsync("k1");
        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();

        // Clear with a watermark strictly below any current mark - the
        // existing entry must remain.
        await grain.ClearDirtyLeavesUpToAsync(default);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Is.Not.Empty);
    }

    [Test]
    public async Task ClearDirtyLeavesUpToAsync_is_noop_on_already_drained_state()
    {
        var (grain, state, _, _) = CreateGrain();

        var writesBefore = state.WriteCount;
        await grain.ClearDirtyLeavesUpToAsync(default);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Is.Empty);
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task ClearDirtyLeavesUpToAsync_persists_pending_marks_in_a_single_write()
    {
        // U9h-B: the admin-path Clear call carries any in-memory pending
        // marks to storage on the same WriteStateAsync that records the
        // new watermark. After three Deletes the dirty dict already
        // contains the leaf; ClearDirtyLeavesUpToAsync must persist the
        // trimmed state in exactly one storage write.
        var (grain, state, _, _) = CreateGrain();

        await grain.DeleteAsync("k1");
        await grain.DeleteAsync("k2");
        await grain.DeleteAsync("k3");
        var writesBefore = state.WriteCount;
        Assert.That(writesBefore, Is.Zero, "DeleteAsync must not persist synchronously under coalescing.");

        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();
        await grain.ClearDirtyLeavesUpToAsync(snapshot.ObservedAdvance);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore + 1));
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Is.Empty);
    }

    [Test]
    public async Task GetDirtyLeavesSinceLastCompactionAsync_observes_unpersisted_marks()
    {
        // U9h-B: the compaction coordinator reads the in-memory state
        // directly via the snapshot API, so a leaf that has been routed
        // a Delete but whose mark has not yet been flushed to storage
        // is still discoverable.
        var (grain, state, _, leafId) = CreateGrain();

        await grain.DeleteAsync("k1");
        Assert.That(state.WriteCount, Is.Zero);

        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();

        Assert.That(snapshot.DirtyLeaves, Has.Count.EqualTo(1));
        Assert.That(snapshot.DirtyLeaves[0], Is.EqualTo(leafId));
    }

    [Test]
    public async Task RetainDirtyLeafAsync_lifts_the_mark_above_the_watermark_so_the_drain_preserves_it()
    {
        // Issue 2926: when the compaction walk skips a leaf that will not
        // compact, it must first lift that leaf's dirty mark above the
        // watermark the pass will drain to. Otherwise the shard completes,
        // ClearDirtyLeavesUpToAsync removes every entry at-or-below the
        // advance, and the one leaf that most needed compacting has its
        // dirty signal silently discarded.
        var (grain, state, _, leafId) = CreateGrain();

        await grain.DeleteAsync("k1");
        var snapshot = await grain.GetDirtyLeavesSinceLastCompactionAsync();

        await grain.RetainDirtyLeafAsync(leafId, snapshot.ObservedAdvance);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction[leafId.ToString()],
            Is.GreaterThan(snapshot.ObservedAdvance),
            "the retained mark must be strictly greater than the pass watermark");

        // The drain that follows a completing shard must leave it behind.
        await grain.ClearDirtyLeavesUpToAsync(snapshot.ObservedAdvance);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction.ContainsKey(leafId.ToString()),
            Is.True,
            "a retained leaf survives the drain of the pass that skipped it");
    }

    [Test]
    public async Task RetainDirtyLeafAsync_lifts_a_mark_that_already_exceeds_the_activation_seed()
    {
        // The re-activation shape, and the reason the watermark floor cannot
        // be dropped. MarkLeafDirtyAsync seeds its clock from LastDirtyAdvance,
        // which only moves on a drain. So after a re-activation between the
        // snapshot and the retain - LastDirtyAdvance stale, the leaf's mark
        // (and hence the pass watermark) well above it - a tick from that seed
        // lands far below the watermark, the max-merge keeps the existing mark
        // instead, and that mark is exactly the watermark: not strictly above
        // it, and so squarely in the drain's path.
        var leafId = GrainId.Create("leaf", "dirty-tree-leaf-0");
        var existingMark = new HybridLogicalClock
        {
            WallClockTicks = DateTime.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
        };

        var state = new FakePersistentState<ShardRootState>();
        state.State.LastDirtyAdvance = HybridLogicalClock.Zero;
        state.State.DirtyLeavesSinceLastCompaction[leafId.ToString()] = existingMark;

        var (grain, _, _, _) = CreateGrain(state);

        // The watermark the in-flight pass will drain to is the mark it
        // observed, which is the existing one.
        await grain.RetainDirtyLeafAsync(leafId, existingMark);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction[leafId.ToString()],
            Is.GreaterThan(existingMark),
            "a stale activation seed must not let the retained mark stay at the watermark");

        await grain.ClearDirtyLeavesUpToAsync(existingMark);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction.ContainsKey(leafId.ToString()),
            Is.True);
    }

    [Test]
    public async Task RetainDirtyLeafAsync_marks_a_leaf_that_has_no_existing_entry()
    {
        // Retain is called with a leaf id taken from the pass's own snapshot,
        // but that snapshot can outlive a concurrent drain that removed the
        // entry. Retain must then re-create it above the watermark rather
        // than leave the leaf unmarked.
        //
        // This is also the arm that pins the OTHER floor: with no existing
        // mark to floor on, only flooring at `above` keeps the result ahead
        // of a watermark that the activation seed is nowhere near. Hence a
        // watermark deliberately far in the future - one close to `now` would
        // be cleared by an ordinary tick and the clause would prove nothing.
        var (grain, state, _, leafId) = CreateGrain();

        var watermark = new HybridLogicalClock
        {
            WallClockTicks = DateTime.UtcNow.Ticks + TimeSpan.FromHours(1).Ticks,
        };
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Is.Empty);

        await grain.RetainDirtyLeafAsync(leafId, watermark);

        Assert.That(state.State.DirtyLeavesSinceLastCompaction[leafId.ToString()],
            Is.GreaterThan(watermark));
    }
}
