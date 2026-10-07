using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for <see cref="ITreeResizeGrain.HoldsShardSplitsAsync"/> (issue #4478):
/// a resize holds adaptive splits while it is in flight and while an undo is
/// pending, running or not yet persisted, and releases them once it has
/// completed - even while the copy it replaced still mirrors into the resized
/// copy, which keeps holding consolidations and reshards.
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task HoldsShardSplits_is_false_with_no_resize()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.That(await grain.HoldsShardSplitsAsync(), Is.False);
    }

    [Test]
    public async Task HoldsShardSplits_is_true_while_a_resize_is_in_flight()
    {
        var (grain, state, _, _, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Snapshot);

        Assert.That(await grain.HoldsShardSplitsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardSplits_is_false_once_complete_while_the_replaced_copy_still_mirrors()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, HoldResizedTreeId);
        StubMirror(grainFactory, TreeId, 1, HoldResizedTreeId);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.HoldsShardSplitsAsync(), Is.False);
            Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True, "consolidations and reshards stay held");
        });
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).GetMirrorDestinationAsync();
    }

    [Test]
    public async Task HoldsShardSplits_is_true_while_an_undo_is_pending()
    {
        var undo = new FakePersistentState<TreeResizeUndoState>();
        undo.State.RequestedOperationId = HoldOperationId;
        var (grain, state, _, _, _) = CreateGrain(undoState: undo);
        SeedCompletedResize(state, TreeId);

        Assert.That(await grain.HoldsShardSplitsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardSplits_is_true_while_a_state_change_is_not_yet_persisted()
    {
        var (grain, state, _, _, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        Assert.That(await grain.HoldsShardSplitsAsync(), Is.False, "precondition: settled");

        state.State.Complete = false;

        Assert.That(await grain.HoldsShardSplitsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardSplits_is_true_while_an_undo_runs_and_false_once_it_has_completed()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, null);
        var deletion = SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        deletion.IsPhysicalDeletedAsync().Returns(gate.Task);

        var undo = grain.UndoResizeAsync();
        var whileRunning = await grain.HoldsShardSplitsAsync();
        gate.SetResult(false);
        await undo;

        Assert.Multiple(async () =>
        {
            Assert.That(whileRunning, Is.True);
            Assert.That(await grain.HoldsShardSplitsAsync(), Is.False);
        });
    }
}
