using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for <c>UndoResizeAsync</c> entered at every resize
/// phase - <see cref="ResizePhase.Snapshot"/>, <see cref="ResizePhase.Swap"/>,
/// <see cref="ResizePhase.Reject"/> and <see cref="ResizePhase.Cleanup"/> -
/// plus the completed-resize case.
/// <para>
/// The after-swap branch used to call <c>ITreeDeletionGrain.RecoverAsync</c>
/// unconditionally. Only the Cleanup phase ever soft-deletes the old physical
/// tree (and it does so only after <c>RejectOldShardsAsync</c> has already
/// advanced the phase to Cleanup), so in Swap, Reject, and the early part of
/// Cleanup the old tree is still live and the real grain rejects the recovery
/// with <c>Cannot recover a tree that has not been deleted.</c>. Because the
/// recovery was step 1, undo aborted before removing the alias, clearing
/// shadow-forward, deleting the destination tree, restoring the registry entry,
/// or resetting the resize state - and failed identically on every retry.
/// </para>
/// <para>
/// These tests therefore stub <c>RecoverAsync</c> to throw exactly as the real
/// grain does whenever the old tree is not deleted, and assert the full
/// compensation actually ran - not merely that undo stopped throwing.
/// </para>
/// </summary>
public partial class TreeResizeGrainTests
{
    private const string UndoSnapshotSuffix = "undo-phase";

    /// <summary>
    /// Seeds an in-flight resize positioned at <paramref name="phase"/>.
    /// </summary>
    private static void SeedInFlightResize(
        FakePersistentState<TreeResizeState> state, ResizePhase phase)
    {
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = phase;
        state.State.OperationId = UndoSnapshotSuffix;
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/{UndoSnapshotSuffix}";
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.OldRegistryEntry = new TreeRegistryEntry
        {
            MaxLeafKeys = 64,
            MaxInternalChildren = 32,
            ShardCount = ShardCount,
        };
    }

    /// <summary>
    /// Asserts the full after-swap compensation ran: alias removed, registry
    /// entry restored to the pre-resize pin, destination tree deleted,
    /// shadow-forward cleared on every old-tree shard, and resize state reset
    /// so the coordinator is idle again.
    /// </summary>
    private static async Task AssertAfterSwapCompensationCompleteAsync(
        FakePersistentState<TreeResizeState> state, IGrainFactory grainFactory)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Alias removed so the logical tree maps back to the old physical tree.
        await registry.Received(1).RemoveAliasAsync(TreeId);

        // Destination (snapshot) tree deleted.
        await grainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/{UndoSnapshotSuffix}")
            .Received(1).DeleteTreeAsync();

        // Pre-resize registry entry restored.
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.MaxLeafKeys == 64 && e.MaxInternalChildren == 32));

        // Shadow-forward cleared on every old-tree shard so it is writable again.
        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}")
                .Received(1).ClearShadowForwardAsync(UndoSnapshotSuffix);
        }

        // Resize state reset.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.SnapshotTreeId, Is.Null);
            Assert.That(state.State.OldPhysicalTreeId, Is.Null);
            Assert.That(state.State.OldRegistryEntry, Is.Null);
        });
    }

    // --- Snapshot phase (before swap) ---

    [Test]
    public async Task UndoResize_at_snapshot_phase_discards_destination_without_recovering()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Snapshot);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        // Drain-window undo: abort the snapshot, clear shadow-forward, delete
        // the draft destination. The source tree is untouched - never deleted,
        // so never recovered, and no alias was ever set.
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId)
            .Received(1).AbortAsync(UndoSnapshotSuffix);
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().RecoverAsync();
        await grainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/{UndoSnapshotSuffix}")
            .Received(1).DeleteTreeAsync();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().RemoveAliasAsync(Arg.Any<string>());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.SnapshotTreeId, Is.Null);
            Assert.That(state.State.OldPhysicalTreeId, Is.Null);
        });
    }

    // --- Swap phase (old tree still live) ---

    [Test]
    public async Task UndoResize_at_swap_phase_completes_without_recovering_live_old_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Swap);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().RecoverAsync();
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    // --- Reject phase (old tree still live) ---

    [Test]
    public async Task UndoResize_at_reject_phase_completes_without_recovering_live_old_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().RecoverAsync();
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    // --- Cleanup phase, before the soft delete has actually happened ---

    [Test]
    public async Task UndoResize_at_cleanup_phase_before_soft_delete_completes_without_recovering()
    {
        // RejectOldShardsAsync advances the phase to Cleanup and only then does
        // CleanupOldTreeAsync soft-delete the old tree, so Phase == Cleanup does
        // not by itself imply the tree is deleted.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Cleanup);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().RecoverAsync();
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    // --- Cleanup phase, after the soft delete ---

    [Test]
    public async Task UndoResize_at_cleanup_phase_after_soft_delete_recovers_old_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Cleanup);
        SetupOldTreeDeletion(grainFactory, isDeleted: true);

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .Received(1).RecoverAsync();
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    // --- Completed resize (soft-delete window) ---

    [Test]
    public async Task UndoResize_after_completed_resize_recovers_soft_deleted_old_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Cleanup);
        state.State.InProgress = false;
        state.State.Complete = true;
        SetupOldTreeDeletion(grainFactory, isDeleted: true);

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .Received(1).RecoverAsync();
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    // --- Genuine recovery failures still surface ---

    [Test]
    public void UndoResize_propagates_recovery_failure_when_old_tree_already_purged()
    {
        // The conditional probe must not become a blanket swallow: when the old
        // tree really was soft-deleted but its data has since been purged,
        // recovery is genuinely impossible and the caller must be told rather
        // than handed a silently half-completed undo.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Cleanup);

        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        deletion.IsDeletedAsync().Returns(Task.FromResult(true));
        deletion.RecoverAsync().ThrowsAsync(
            new InvalidOperationException("Cannot recover a tree whose data has already been purged."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());
        Assert.That(ex!.Message, Does.Contain("already been purged"));
    }

    // --- Retryability ---
    [Test]
    public async Task UndoResize_at_swap_phase_is_idempotent_across_repeated_calls()
    {
        // The original defect made undo permanently unretryable: it threw on
        // step 1 every time, so the tree stayed wedged mid-resize. Once the
        // first undo succeeds the state is reset, so a second call must be
        // refused cleanly by the top guard rather than throwing an internal
        // recovery fault.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Swap);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);

        await grain.UndoResizeAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());
        Assert.That(ex!.Message, Does.Contain("No resize exists for tree"));
    }
}
