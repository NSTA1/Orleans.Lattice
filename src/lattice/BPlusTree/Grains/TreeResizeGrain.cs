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
    ILogger<TreeResizeGrain> logger,
    [PersistentState("tree-resize", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeResizeState> state) : ITreeResizeGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "resize-keepalive";

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _resizeTimer;

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
        // Resize crosses physical trees; a concurrent reshard mutating the
        // source tree's ShardMap would invalidate the snapshot's per-slot
        // routing assumptions.
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        if (!await reshard.IsCompleteAsync())
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}'; resize refused until reshard completes.");

        if (state.State.Complete)
        {
            // Reset completion flag for a new resize.
            state.State.Complete = false;
        }

        await InitiateResizeStateAsync(newMaxLeafKeys, newMaxInternalChildren);
        await StartResizeAsync();
    }

    /// <summary>
    /// Persists the resize intent with <see cref="ResizePhase.Snapshot"/> phase,
    /// then kicks off the offline snapshot to the new physical tree.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateResizeStateAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var options = Options;
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
        state.State.ShardCount = options.ShardCount;
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
            newMaxLeafKeys, newMaxInternalChildren, operationId);
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

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == KeepaliveReminderName)
        {
            if (state.State.InProgress && _resizeTimer is null)
            {
                await StartResizeTimerAsync();
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    public async Task UndoResizeAsync()
    {
        // Undo is available in two windows:
        //   1. Before swap — while the online snapshot is draining and the
        //      alias has not yet been updated. Shadow-forwarding is active
        //      but discardable: clearing it and deleting the draft
        //      destination tree leaves the source fully intact.
        //   2. After swap — during the SoftDeleteDuration window. Recover
        //      the old physical tree, remove the alias, restore registry
        //      entry, and delete the destination tree. Shadow-forward
        //      state on the old-tree shards must also be cleared so the
        //      tree becomes writable again.
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

        if (state.State.InProgress)
        {
            // ---- Undo during drain (before swap). ----
            // Cancel the in-flight snapshot by clearing shadow-forward on
            // every old-tree shard and deleting the destination tree.
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

            _resizeTimer?.Dispose();
            _resizeTimer = null;
            await UnregisterKeepaliveAsync();
            this.DeactivateOnIdle();
            return;
        }

        // ---- Undo after swap. ----
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

    private async Task StartResizeAsync()
    {
        // Register keepalive for crash recovery.
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        await StartResizeTimerAsync();
    }

    private Task StartResizeTimerAsync()
    {
        _resizeTimer = this.RegisterGrainTimer(
            OnResizeTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
        return Task.CompletedTask;
    }

    private async Task OnResizeTimerTick(CancellationToken ct)
    {
        await ProcessNextPhaseAsync();
    }

    /// <summary>
    /// Processes the next phase of the resize. Exposed as <c>internal</c> for
    /// unit testing.
    /// </summary>
    internal async Task ProcessNextPhaseAsync()
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
            logger.LogWarning(ex, "Resize phase {Phase} failed for tree {TreeId}",
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

        // Update registry entry with new sizing.
        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = state.State.NewMaxLeafKeys,
            MaxInternalChildren = state.State.NewMaxInternalChildren,
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

        // Only soft-delete if the old physical differs from the logical tree ID.
        // If they're the same, the old tree's shards are the ones being aliased away
        // from — soft-delete them.
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        await deletion.DeleteTreeAsync();
    }

    internal async Task CompleteResizeAsync()
    {
        _resizeTimer?.Dispose();
        _resizeTimer = null;

        state.State.InProgress = false;
        state.State.Complete = true;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();

        this.DeactivateOnIdle();
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister resize keepalive reminder for tree {TreeId}", TreeId);
        }
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(!state.State.InProgress);
}
