using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4518: the after-swap unwind of a replicated tree is refused before
/// anything is armed, and the decision is taken once per operation, so a retried
/// unwind that may already have armed the resized copy always finishes.
/// </summary>
public partial class TreeResizeGrainTests
{
    private sealed class ReplicatedTreeContext(bool replicated) : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => "test";

        public LatticeMergeMode? ResolveMergeMode(string treeId) => replicated ? LatticeMergeMode.LwwRegister : null;
    }

    private static ILatticeRegistry AliasOnResizedCopy(IGrainFactory grainFactory)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var row = new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            PhysicalTreeId = $"{TreeId}/resized/{UndoSnapshotSuffix}",
            ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount),
        };
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(row));
        registry.SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>())
            .Returns(Task.FromResult<TreeRegistryEntry?>(row));
        return registry;
    }

    [Test]
    public async Task UndoResize_after_the_swap_of_a_replicated_tree_is_refused_before_anything_is_armed()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain(replicationContext: new ReplicatedTreeContext(true));
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var registry = AliasOnResizedCopy(grainFactory);

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.That(refusal!.Message, Does.Contain("replicated"));
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/resized/{UndoSnapshotSuffix}/0")
            .DidNotReceive().MarkRetainedRedirectAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
        await registry.DidNotReceive().SwapAliasAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
        Assert.That(state.State.InProgress, Is.True, "the resize is left as it was");
    }

    [Test]
    public async Task UndoResize_after_the_swap_of_an_unreplicated_tree_records_its_clearance_and_runs()
    {
        var undo = new FakePersistentState<TreeResizeUndoState>();
        var (grain, state, _, grainFactory, _) = CreateGrain(
            undoState: undo, replicationContext: new ReplicatedTreeContext(false));
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var registry = AliasOnResizedCopy(grainFactory);

        await grain.UndoResizeAsync();

        Assert.That(undo.State.UnwindClearedOperationId, Is.EqualTo(UndoSnapshotSuffix));
        await registry.Received(1).SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task UndoResize_retried_after_its_clearance_finishes_even_once_the_tree_is_replicated()
    {
        // An earlier attempt was cleared and may have armed the resized copy before
        // it failed; the tree then became replicated. The retry must finish, or the
        // armed copy the alias still names would refuse every routed call.
        var undo = new FakePersistentState<TreeResizeUndoState>();
        undo.State.UnwindClearedOperationId = UndoSnapshotSuffix;
        var (grain, state, _, grainFactory, _) = CreateGrain(
            undoState: undo, replicationContext: new ReplicatedTreeContext(true));
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var registry = AliasOnResizedCopy(grainFactory);

        await grain.UndoResizeAsync();

        await registry.Received(1).SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task UndoResize_of_a_replicated_tree_whose_alias_never_moved_is_not_refused()
    {
        // A resize held at the swap (its flip refused) never served the resized
        // copy, so the shipper never read it and the undo is safe.
        var (grain, state, _, grainFactory, _) = CreateGrain(replicationContext: new ReplicatedTreeContext(true));
        SeedInFlightResize(state, ResizePhase.Swap);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        Assert.That(state.State.InProgress, Is.False, "the unwind ran to completion");
    }
}
