using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue 3923 on the facade side: <see cref="ILattice.UndoResizeAsync"/> accepts
/// the undo through the interleaved request and then waits only a bounded time
/// for the coordinator's phase loop to unwind it, instead of holding the caller
/// for the whole unwind behind the coordinator's turn.
/// </summary>
public partial class LatticeGrainTests
{
    private static ITreeResizeGrain SetupResizeCoordinator(IGrainFactory factory, params ResizeUndoProgress[] progress)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        factory.GetGrain<ITreeResizeGrain>("my-tree", Arg.Any<string>()).Returns(resize);
        resize.RequestUndoAsync().Returns(Task.FromResult("op-1"));
        if (progress.Length > 0)
            resize.GetUndoProgressAsync().Returns(progress[0], progress[1..]);
        return resize;
    }

    [Test]
    public async Task UndoResizeAsync_requests_the_undo_and_returns_once_it_has_unwound()
    {
        var (grain, factory) = CreateGrain();
        var resize = SetupResizeCoordinator(factory,
            new ResizeUndoProgress(true, null, null),
            new ResizeUndoProgress(false, null, null));

        await grain.UndoResizeAsync();

        await resize.Received(1).RequestUndoAsync();
        await resize.Received(2).GetUndoProgressAsync();
        await resize.DidNotReceive().UndoResizeAsync();
    }

    [Test]
    public void UndoResizeAsync_surfaces_the_reason_an_accepted_undo_was_withdrawn()
    {
        var (grain, factory) = CreateGrain();
        SetupResizeCoordinator(factory,
            new ResizeUndoProgress(false, "op-1", "Cannot recover a tree whose data has already been purged."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.That(ex!.Message, Does.Contain("already been purged"));
    }

    [Test]
    public async Task UndoResizeAsync_ignores_a_failure_recorded_for_a_different_resize()
    {
        var (grain, factory) = CreateGrain();
        SetupResizeCoordinator(factory, new ResizeUndoProgress(false, "an-older-op", "old failure"));

        await grain.UndoResizeAsync();
    }

    [Test]
    public async Task UndoResizeAsync_returns_with_the_undo_still_unwinding_once_its_wait_budget_is_spent()
    {
        // Returning is the accept-then-poll contract: the caller is answered
        // inside its response timeout and follows the unwind on the status surface.
        var (grain, factory) = CreateGrain();
        var resize = SetupResizeCoordinator(factory, new ResizeUndoProgress(true, null, null));
        var started = Environment.TickCount64;

        await grain.UndoResizeAsync();

        var elapsed = TimeSpan.FromMilliseconds(Environment.TickCount64 - started);
        Assert.That(elapsed, Is.GreaterThanOrEqualTo(LatticeGrain.ResizeUndoWaitBudget - TimeSpan.FromMilliseconds(100))
            .And.LessThan(TimeSpan.FromSeconds(30)));
        await resize.Received(1).RequestUndoAsync();
    }

    [Test]
    public void UndoResizeAsync_stops_waiting_when_the_caller_cancels()
    {
        var (grain, factory) = CreateGrain();
        SetupResizeCoordinator(factory, new ResizeUndoProgress(true, null, null));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        Assert.CatchAsync<OperationCanceledException>(() => grain.UndoResizeAsync(cts.Token));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task IsResizeUndoPendingAsync_reports_the_coordinator_undo_progress(bool pending)
    {
        var (grain, factory) = CreateGrain();
        SetupResizeCoordinator(factory, new ResizeUndoProgress(pending, null, null));

        Assert.That(await grain.IsResizeUndoPendingAsync(), Is.EqualTo(pending));
    }
}
