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
}
