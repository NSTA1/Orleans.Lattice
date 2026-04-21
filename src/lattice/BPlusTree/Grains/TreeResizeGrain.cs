using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree resizing by taking an online snapshot to a new physical tree,
/// swapping the tree alias, and soft-deleting the old physical tree.
/// <para>
/// The resize flow is:
/// <list type="number">
/// <item><description><see cref="ResizePhase.Snapshot"/> — online snapshot of the logical
/// tree to a new physical tree with the desired sizing. The source tree remains
/// fully available for reads and writes; every accepted mutation is
/// shadow-forwarded to the destination.</description></item>
/// <item><description><see cref="ResizePhase.Swap"/> — set alias so the logical tree ID
/// points to the new physical tree.</description></item>
/// <item><description><see cref="ResizePhase.Reject"/> — transition every shard on the old
/// physical tree to the Rejecting phase so any lingering client request to the old
/// tree throws <see cref="StaleTreeRoutingException"/> and retries against the new
/// alias target.</description></item>
/// <item><description><see cref="ResizePhase.Cleanup"/> — soft-delete the old physical
/// tree to reclaim storage.</description></item>
/// </list>
/// During the <see cref="LatticeOptions.SoftDeleteDuration"/> window, the resize
/// can be undone with <see cref="UndoResizeAsync"/>. Undo is also available
/// before the alias swap (during the drain) — in that case the destination
/// tree is deleted and shadow-forward cleared without touching the source.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TreeResizeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeResizeGrain> logger,
    [PersistentState("tree-resize", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeResizeState> state)
    : CoordinatorGrain<TreeResizeGrain>(context, reminderRegistry, logger), ITreeResizeGrain
{
    private string TreeId => Context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "resize-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeId}";

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        if (newMaxLeafKeys <= 1)
            throw new ArgumentOutOfRangeException(nameof(newMaxLeafKeys), "Must be greater than 1.");
        if (newMaxInternalChildren <= 2)
            throw new ArgumentOutOfRangeException(nameof(newMaxInternalChildren), "Must be greater than 2.");

        if (state.State.InProgress)
        {
            // Idempotent if same parameters.
            if (state.State.NewMaxLeafKeys == newMaxLeafKeys &&
                state.State.NewMaxInternalChildren == newMaxInternalChildren)
                return;

            throw new InvalidOperationException(
                $"A resize is already in progress for tree '{TreeId}' with different parameters " +
                $"(MaxLeafKeys={state.State.NewMaxLeafKeys}, MaxInternalChildren={state.State.NewMaxInternalChildren}).");
        }

        // Interlock: refuse to start a resize while a reshard is in flight.
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        if (!await reshard.IsIdleAsync())
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}'; resize refused until reshard completes.");

        if (state.State.Complete)
        {
            state.State.Complete = false;
        }

        // F-019c empty-tree fast-path: if the tree has no live entries, repin
        // the structural leaf/internal sizes atomically on the registry and
        // short-circuit the coordinator machinery. No snapshot, no shadow-
        // forward, no alias swap.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        var liveCount = await lattice.CountAsync();
        if (liveCount == 0)
        {
            await ApplyEmptyTreeResizeAsync(newMaxLeafKeys, newMaxInternalChildren);
            return;
        }

        await InitiateResizeStateAsync(newMaxLeafKeys, newMaxInternalChildren);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// F-019c empty-tree fast-path: repins <c>MaxLeafKeys</c> /
    /// <c>MaxInternalChildren</c> in the registry without running the online
    /// resize pipeline.
    /// </summary>
    private async Task ApplyEmptyTreeResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var existing = await registry.GetEntryAsync(TreeId);
        var updated = (existing ?? new State.TreeRegistryEntry()) with
        {
            MaxLeafKeys = newMaxLeafKeys,
            MaxInternalChildren = newMaxInternalChildren,
        };
        await registry.UpdateAsync(TreeId, updated);

        state.State.Complete = true;
        state.State.NewMaxLeafKeys = newMaxLeafKeys;
        state.State.NewMaxInternalChildren = newMaxInternalChildren;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Persists the resize intent with <see cref="ResizePhase.Snapshot"/> phase,
    /// then kicks off the offline snapshot to the new physical tree.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateResizeStateAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var operationId = Guid.NewGuid().ToString("N");

        // Resolve the current physical tree ID (may already be aliased from a prior resize).
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var currentPhysical = await registry.ResolveAsync(TreeId);
        var snapshotTreeId = $"{TreeId}/resized/{operationId}";

        // Capture the old registry entry so UndoResizeAsync can restore it.
        var oldEntry = await registry.GetEntryAsync(TreeId);

        // Persist intent BEFORE any external side effects.
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.NewMaxLeafKeys = newMaxLeafKeys;
        state.State.NewMaxInternalChildren = newMaxInternalChildren;
        state.State.OperationId = operationId;
        state.State.ShardCount = resolved.ShardCount;
        state.State.Complete = false;
        state.State.SnapshotTreeId = snapshotTreeId;
        state.State.OldPhysicalTreeId = currentPhysical;
        state.State.OldRegistryEntry = oldEntry;
        await state.WriteStateAsync();

        // Initiate the online snapshot from current physical tree to new tree.
        // Online mode keeps the source tree available for reads and writes
        // throughout the resize via the shadow-forwarding primitive. The
        // resize's operationId is threaded into the snapshot so both
        // coordinators stamp the same id onto the source shards' shadow-
        // forward state — the resize can then later call
        // EnterRejectingAsync / ClearShadowForwardAsync with this same id.
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(currentPhysical);
        await snapshot.SnapshotWithOperationIdAsync(snapshotTreeId, SnapshotMode.Online,
            newMaxLeafKeys, newMaxInternalChildren, operationId, TreeId);
    }

    public async Task RunResizePassAsync()
    {
        if (!state.State.InProgress) return;

        if (state.State.Phase == ResizePhase.Snapshot)
        {
            await WaitForSnapshotAsync();
        }

        if (state.State.Phase == ResizePhase.Swap)
        {
            await SwapAliasAsync();
        }

        if (state.State.Phase == ResizePhase.Reject)
        {
            await RejectOldShardsAsync();
        }

        if (state.State.Phase == ResizePhase.Cleanup)
        {
            await CleanupOldTreeAsync();
        }

        await CompleteResizeAsync();
    }

    public async Task UndoResizeAsync()
    {
        // Undo is available in two windows:
        //   1. Before swap — while the online snapshot is draining and the
        //      alias has not yet been updated. Shadow-forwarding is active
        //      but discardable: clearing it and deleting the draft
        //      destination tree leaves the source fully intact. This window
        //      corresponds strictly to Phase == Snapshot; once Swap begins
        //      the alias has been flipped and the destination is live.
        //   2. After swap — during the SoftDeleteDuration window, or mid
        //      Swap/Reject/Cleanup. Recover the old physical tree, remove
        //      the alias, restore registry entry, and delete the destination
        //      tree. Shadow-forward state on the old-tree shards must also
        //      be cleared so the tree becomes writable again.
        if (!state.State.InProgress && !state.State.Complete)
            throw new InvalidOperationException(
                $"No resize exists for tree '{TreeId}' that can be undone.");

        if (state.State.OldPhysicalTreeId is null || state.State.SnapshotTreeId is null)
            throw new InvalidOperationException(
                $"Resize state for tree '{TreeId}' is incomplete; cannot undo.");

        var oldPhysical = state.State.OldPhysicalTreeId;
        var snapshotTreeId = state.State.SnapshotTreeId;
        var opId = state.State.OperationId!;
        var shardCount = state.State.ShardCount;

        // Drain-window undo applies only while Phase == Snapshot. Phases Swap,
        // Reject, and Cleanup all occur after the alias flip, and must follow
        // the after-swap recovery path — routing them through the drain
        // branch would erroneously delete the live destination tree.
        var isBeforeSwap = state.State.InProgress && state.State.Phase == ResizePhase.Snapshot;
        if (isBeforeSwap)
        {
            // ---- Undo during drain (before swap). ----
            // Cancel the in-flight snapshot by telling its coordinator to
            // tear itself down, then clear shadow-forward on every old-tree
            // shard and delete the destination tree. No alias was ever set
            // so no recovery or alias removal needed.
            //
            // Race note: a ClearShadowForwardAsync that lands between a
            // shard's TryGetShadowTarget resolving and its forward task
            // running may still result in a write landing on the destination
            // tree just before DeleteTreeAsync is processed. This is safe —
            // the destination is being torn down, so the leaked write is
            // discarded, and forward writes are LWW-idempotent so no
            // source-tree state is corrupted even if that write is later
            // retried against a recovered destination.
            var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(oldPhysical);
            await snapshot.AbortAsync(opId);

            // No alias was ever set so no recovery or alias removal needed.
            var clearTasks = new Task[shardCount];
            for (int i = 0; i < shardCount; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{i}");
                clearTasks[i] = shard.ClearShadowForwardAsync(opId);
            }
            await Task.WhenAll(clearTasks);

            var destDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
            await destDeletion.DeleteTreeAsync();

            ResetResizeState();
            await state.WriteStateAsync();

            await CompleteCoordinatorAsync();
            return;
        }

        // ---- Undo after swap. ----
        // Defensively abort any snapshot activation that may have been
        // resurrected by crash recovery — a no-op when the snapshot has
        // already completed or when the opId no longer matches.
        var postSwapSnapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(oldPhysical);
        await postSwapSnapshot.AbortAsync(opId);

        // 1. Recover the old physical tree from soft-delete.
        var oldDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        await oldDeletion.RecoverAsync();

        // 2. Clear shadow-forward on every old-tree shard so the tree becomes
        //    writable again (lifts the Rejecting phase).
        var undoTasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{i}");
            undoTasks[i] = shard.ClearShadowForwardAsync(opId);
        }
        await Task.WhenAll(undoTasks);

        // 3. Remove the alias so the logical tree maps back to the old physical tree.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RemoveAliasAsync(TreeId);

        // 4. Delete the snapshot tree.
        var newDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
        await newDeletion.DeleteTreeAsync();

        // 5. Restore the original registry entry (or clear overrides if none existed).
        await registry.UpdateAsync(TreeId, state.State.OldRegistryEntry ?? new TreeRegistryEntry());

        // 6. Clear resize state.
        ResetResizeState();
        await state.WriteStateAsync();
    }

    private void ResetResizeState()
    {
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.SnapshotTreeId = null;
        state.State.OldPhysicalTreeId = null;
        state.State.OldRegistryEntry = null;
    }

    /// <summary>
    /// Processes the next phase of the resize. Exposed as <c>internal</c> via
    /// <c>protected</c> override for unit testing.
    /// </summary>
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        try
        {
            switch (state.State.Phase)
            {
                case ResizePhase.Snapshot:
                    await WaitForSnapshotAsync();
                    break;

                case ResizePhase.Swap:
                    await SwapAliasAsync();
                    break;

                case ResizePhase.Reject:
                    await RejectOldShardsAsync();
                    break;

                case ResizePhase.Cleanup:
                    await CleanupOldTreeAsync();
                    await CompleteResizeAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Resize phase {Phase} failed for tree {TreeId}",
                state.State.Phase, TreeId);
        }
    }

    /// <summary>
    /// Waits for the snapshot to complete by calling <c>RunSnapshotPassAsync</c>.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task WaitForSnapshotAsync()
    {
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(state.State.OldPhysicalTreeId!);
        await snapshot.RunSnapshotPassAsync();

        state.State.Phase = ResizePhase.Swap;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Swaps the alias so the logical tree ID now points to the snapshot tree.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task SwapAliasAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Update registry entry with new structural sizing. Preserve the
        // previously-pinned ShardCount so F-019c's resolver does not see a
        // null pin on the logical tree after the swap.
        var oldEntry = state.State.OldRegistryEntry;
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = state.State.NewMaxLeafKeys,
            MaxInternalChildren = state.State.NewMaxInternalChildren,
            ShardCount = oldEntry?.ShardCount ?? state.State.ShardCount,
        };
        await registry.UpdateAsync(TreeId, entry);

        // Set alias to redirect to the new physical tree.
        await registry.SetAliasAsync(TreeId, state.State.SnapshotTreeId!);

        state.State.Phase = ResizePhase.Reject;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Transitions every shard on the old physical tree to
    /// <c>ShadowForwardPhase.Rejecting</c>. Any lingering client request
    /// routed to the old tree after this point throws
    /// <see cref="StaleTreeRoutingException"/>, which the stateless
    /// <see cref="LatticeGrain"/> routing tier handles by refreshing its
    /// alias and retrying against the new physical tree. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task RejectOldShardsAsync()
    {
        var oldPhysical = state.State.OldPhysicalTreeId!;
        var opId = state.State.OperationId!;
        var shardCount = state.State.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{i}");
            tasks[i] = shard.EnterRejectingAsync(opId);
        }
        await Task.WhenAll(tasks);

        state.State.Phase = ResizePhase.Cleanup;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Soft-deletes the old physical tree. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task CleanupOldTreeAsync()
    {
        var oldPhysical = state.State.OldPhysicalTreeId!;

        // The alias now repoints the logical tree to SnapshotTreeId, so the
        // old physical tree receives no further public traffic — soft-delete
        // it to reclaim storage. The SoftDeleteDuration window keeps the
        // data intact so UndoResizeAsync can still restore it.
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        await deletion.DeleteTreeAsync();
    }

    internal async Task CompleteResizeAsync()
    {
        state.State.InProgress = false;
        state.State.Complete = true;
        await state.WriteStateAsync();

        await CompleteCoordinatorAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!state.State.InProgress);
}
