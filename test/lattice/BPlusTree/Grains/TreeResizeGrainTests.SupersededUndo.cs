using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeResizeGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task UndoResize_after_a_later_alias_cutover_refuses_before_compensation(bool cleared)
    {
        var undo = new FakePersistentState<TreeResizeUndoState>();
        if (cleared) undo.State.UnwindClearedOperationId = UndoSnapshotSuffix;
        var (grain, state, _, grainFactory, _) = CreateGrain(undoState: undo);
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var registry = AliasOnResizedCopy(grainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                ShardCount = ShardCount,
                PhysicalTreeId = "later-restored-copy",
                ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount),
            }));

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.That(refusal!.Message, Does.Contain("later-restored-copy"));
        await registry.DidNotReceive().SwapAliasAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId)
            .DidNotReceive().AbortAsync(Arg.Any<string>());
        Assert.That(state.State.InProgress, Is.True);
    }

    [Test]
    public async Task UndoResize_fences_its_swap_with_the_physical_identity_it_read()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var registry = AliasOnResizedCopy(grainFactory);

        await grain.UndoResizeAsync();

        await registry.Received(1).SwapAliasAsync(
            TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(),
            $"{TreeId}/resized/{UndoSnapshotSuffix}");
    }
}
