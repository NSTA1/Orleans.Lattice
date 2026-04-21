using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Phase 3 (online resize) coverage: the resize↔reshard interlock,
/// the post-swap <see cref="ResizePhase.Reject"/> fan-out, shared
/// operationId handoff to the driven snapshot, and the dual-path
/// <c>UndoResizeAsync</c> flow (during-drain vs after-swap).
/// </summary>
public partial class TreeResizeGrainTests
{
    // --- Resize ↔ Reshard interlock ---

    [Test]
    public void ResizeAsync_refused_while_reshard_in_flight()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        reshard.IsIdleAsync().Returns(Task.FromResult(false));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ResizeAsync(256, 64));
    }

    [Test]
    public void ResizeAsync_validates_arguments_before_consulting_reshard_interlock()
    {
        // Invalid argument must throw ArgumentOutOfRangeException even when
        // a reshard is in flight — caller feedback on bad input must not be
        // masked by an interlock check.
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        reshard.IsIdleAsync().Returns(Task.FromResult(false));

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.ResizeAsync(1, 64));
    }

    // --- Shared operationId with the driven snapshot ---

    [Test]
    public async Task InitiateResize_shares_operationId_with_snapshot()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.InitiateResizeStateAsync(256, 64);

        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await snapshot.Received(1).SnapshotWithOperationIdAsync(
            state.State.SnapshotTreeId!,
            SnapshotMode.Online,
            256, 64,
            state.State.OperationId!, TreeId);
    }

    // --- Reject phase fan-out ---

    [Test]
    public async Task RejectOldShards_flips_all_old_shards_to_Rejecting_with_shared_opId()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Reject;
        state.State.OperationId = "reject-op";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;

        await grain.RejectOldShardsAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).EnterRejectingAsync("reject-op");
        }
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Cleanup));
    }

    // --- UndoResizeAsync during drain (pre-swap) ---

    [Test]
    public async Task UndoResize_during_drain_clears_shadow_forward_on_all_old_shards()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain";

        await grain.UndoResizeAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).ClearShadowForwardAsync("undo-drain");
        }
    }

    [Test]
    public async Task UndoResize_during_drain_deletes_destination_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain";

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/undo-drain")
            .Received(1).DeleteTreeAsync();
    }

    [Test]
    public async Task UndoResize_during_drain_does_not_recover_or_remove_alias()
    {
        // No alias was ever set during drain, so undo must not touch the
        // old physical tree's soft-deletion state nor the registry alias.
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain";

        await grain.UndoResizeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .DidNotReceive().RecoverAsync();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().RemoveAliasAsync(Arg.Any<string>());
    }

    [Test]
    public async Task UndoResize_during_drain_resets_resize_state()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain";

        await grain.UndoResizeAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.False);
        Assert.That(state.State.SnapshotTreeId, Is.Null);
        Assert.That(state.State.OldPhysicalTreeId, Is.Null);
    }

    // --- UndoResizeAsync after swap (post-swap, pre-cleanup window) ---

    [Test]
    public async Task UndoResize_after_swap_clears_shadow_forward_on_all_old_shards()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.OperationId = "undo-swap";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-swap";

        await grain.UndoResizeAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).ClearShadowForwardAsync("undo-swap");
        }
    }

    // --- UndoResizeAsync guards ---

    [Test]
    public void UndoResize_throws_when_neither_in_progress_nor_complete()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = false;
        state.State.Complete = false;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.UndoResizeAsync());
    }

    [Test]
    public void UndoResize_throws_when_state_incomplete_missing_old_physical()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.OldPhysicalTreeId = null;
        state.State.SnapshotTreeId = $"{TreeId}/resized/x";

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.UndoResizeAsync());
    }

    [Test]
    public void UndoResize_throws_when_state_incomplete_missing_snapshot_tree_id()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = null;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.UndoResizeAsync());
    }

    // --- UndoResizeAsync phase-aware routing (T-7) ---

    [Test]
    public async Task UndoResize_during_reject_phase_routes_through_after_swap_recovery()
    {
        // During Phase == Reject the alias has already been flipped to the
        // destination tree, so undo must take the after-swap path: recover
        // the old physical tree, remove the alias, clear shadow-forward on
        // every old-tree shard, and delete the snapshot (destination) tree.
        // Routing through the drain branch would erroneously delete the
        // live destination that the alias now points at.
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Phase = ResizePhase.Reject;
        state.State.OperationId = "undo-reject";
        state.State.ShardCount = ShardCount;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-reject";

        await grain.UndoResizeAsync();

        // After-swap branch: recover old tree, delete snapshot (destination) tree.
        var oldDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        var newDeletion = grainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/undo-reject");
        await oldDeletion.Received().RecoverAsync();
        await newDeletion.Received().DeleteTreeAsync();

        // Alias removed so the logical tree maps back to the old physical tree.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received().RemoveAliasAsync(TreeId);

        // Shadow-forward cleared on every old-tree shard.
        for (int i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).ClearShadowForwardAsync("undo-reject");
        }

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.False);
    }
}
