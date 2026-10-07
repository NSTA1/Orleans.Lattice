using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class TreeResizeGrainIsCompleteTests
{
    private const string TreeId = "test-tree";
    private const int ShardCount = 2;

    private static TreeResizeGrain CreateGrainForIsComplete(
        FakePersistentState<TreeResizeState>? existingState = null,
        FakePersistentState<TreeResizeUndoState>? undoState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("resize", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var state = existingState ?? new FakePersistentState<TreeResizeState>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(TreeId));
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);

        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        grainFactory.GetGrain<ITreeSnapshotGrain>(Arg.Any<string>()).Returns(snapshot);

        return new TreeResizeGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeResizeGrain>(),
            Substitute.For<ITagIndexReconcileTrigger>(), state, undoState ?? new FakePersistentState<TreeResizeUndoState>());
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_when_no_resize_initiated()
    {
        var grain = CreateGrainForIsComplete();
        var result = await grain.IsIdleAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_when_resize_in_progress()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = true;
        existingState.State.Phase = ResizePhase.Snapshot;
        var grain = CreateGrainForIsComplete(existingState);
        var result = await grain.IsIdleAsync();
        Assert.That(result, Is.False);
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_after_resize_completes()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = false;
        existingState.State.Complete = true;
        var grain = CreateGrainForIsComplete(existingState);
        var result = await grain.IsIdleAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsIdleAsync_returns_false_while_a_completed_resize_still_holds_its_alias_reservation()
    {
        // Issue #4527: completion is persisted before the reservation is
        // released, so idle must wait for the release or a delete issued on it
        // is refused as "alias operation in progress".
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = false;
        existingState.State.Complete = true;
        existingState.State.AliasReservationId = "resize:held";
        var grain = CreateGrainForIsComplete(existingState);
        var result = await grain.IsIdleAsync();
        Assert.That(result, Is.False);
    }

    [Test]
    public async Task IsIdleAsync_keeps_a_completed_resize_complete_while_an_accepted_undo_holds_the_reservation()
    {
        // The reservation an undo of a completed resize takes is the undo's, not
        // the completion's: the documented contract is that such an undo leaves
        // the resize reading complete.
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.Complete = true;
        existingState.State.OperationId = "op-1";
        existingState.State.AliasReservationId = "resize:undo";
        var undoState = new FakePersistentState<TreeResizeUndoState>();
        undoState.State.RequestedOperationId = "op-1";
        var grain = CreateGrainForIsComplete(existingState, undoState);
        Assert.That(await grain.IsIdleAsync(), Is.True);
    }
}
