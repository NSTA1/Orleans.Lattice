using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeResizeGrain.GetStatusAsync"/>: the
/// fused status read must answer exactly what the three separate reads it replaces
/// (<c>IsIdleAsync</c>, <c>GetUndoProgressAsync().Pending</c>, <c>GetProgressAsync</c>)
/// answer for the same coordinator state.
/// </summary>
public partial class TreeResizeGrainTests
{
    private static async Task AssertStatusMatchesSeparateReadsAsync(
        Orleans.Lattice.BPlusTree.Grains.TreeResizeGrain grain)
    {
        var status = await grain.GetStatusAsync();
        var idle = await grain.IsIdleAsync();
        var undo = await grain.GetUndoProgressAsync();
        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.Idle, Is.EqualTo(idle));
            Assert.That(status.UndoPending, Is.EqualTo(undo.Pending));
            Assert.That(status.Progress, Is.EqualTo(progress));
        });
    }

    [Test]
    public async Task GetStatusAsync_matches_the_separate_reads_for_an_idle_coordinator()
    {
        var (grain, _, _, _, _) = CreateGrain();
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var status = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.Idle, Is.True);
            Assert.That(status.UndoPending, Is.False);
            Assert.That(status.Progress.InProgress, Is.False);
        });
        await AssertStatusMatchesSeparateReadsAsync(grain);
    }

    [Test]
    public async Task GetStatusAsync_matches_the_separate_reads_while_a_resize_copies()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: ResizingState(ResizePhase.Snapshot));
        SnapshotReporting(grainFactory, new SnapshotProgress(true, false, ProgressOperation, SnapshotPhase.Copy, 3, 4));
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var status = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.Idle, Is.False);
            Assert.That(status.UndoPending, Is.False);
            Assert.That(status.Progress.CompletedUnits, Is.EqualTo(3));
        });
        await AssertStatusMatchesSeparateReadsAsync(grain);
    }

    [Test]
    public async Task GetStatusAsync_matches_the_separate_reads_for_an_accepted_undo_of_a_completed_resize()
    {
        var state = new FakePersistentState<TreeResizeState>();
        state.State.Complete = true;
        state.State.OperationId = "op-1";
        state.State.AliasReservationId = "resize:undo";
        var undoState = new FakePersistentState<TreeResizeUndoState>();
        undoState.State.RequestedOperationId = "op-1";
        var (grain, _, _, _, _) = CreateGrain(existingState: state, undoState: undoState);
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var status = await grain.GetStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.Idle, Is.True);
            Assert.That(status.UndoPending, Is.True);
        });
        await AssertStatusMatchesSeparateReadsAsync(grain);
    }
}
