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
    public async Task DeleteAsync_dedups_repeated_marks_within_window()
    {
        var (grain, state, _, _) = CreateGrain();

        await grain.DeleteAsync("k1");
        var writesAfterFirst = state.WriteCount;

        await grain.DeleteAsync("k2");
        await grain.DeleteAsync("k3");

        // Only the first delete should have persisted state; subsequent
        // deletes to the same already-dirty leaf must short-circuit.
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
}
