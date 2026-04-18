using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree resizing by taking an offline snapshot to a new physical tree,
/// swapping the tree alias, and soft-deleting the old physical tree.
/// <para>
/// The resize flow is:
/// <list type="number">
/// <item><description><see cref="ResizePhase.Snapshot"/> — offline snapshot of the logical
/// tree to a new physical tree with the desired sizing.</description></item>
/// <item><description><see cref="ResizePhase.Swap"/> — set alias so the logical tree ID
/// points to the new physical tree.</description></item>
/// <item><description><see cref="ResizePhase.Cleanup"/> — soft-delete the old physical
/// tree to reclaim storage.</description></item>
/// </list>
/// During the <see cref="LatticeOptions.SoftDeleteDuration"/> window, the resize
/// can be undone with <see cref="UndoResizeAsync"/>.
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

        // Initiate the offline snapshot from current physical tree to new tree.
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(currentPhysical);
        await snapshot.SnapshotAsync(snapshotTreeId, SnapshotMode.Offline,
            newMaxLeafKeys, newMaxInternalChildren);
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
        if (!state.State.Complete || state.State.OldPhysicalTreeId is null || state.State.SnapshotTreeId is null)
            throw new InvalidOperationException(
                $"No completed resize exists for tree '{TreeId}' that can be undone.");

        var oldPhysical = state.State.OldPhysicalTreeId;
        var snapshotTreeId = state.State.SnapshotTreeId;

        // 1. Recover the old physical tree from soft-delete.
        var oldDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        await oldDeletion.RecoverAsync();

        // 2. Remove the alias so the logical tree maps back to the old physical tree.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RemoveAliasAsync(TreeId);

        // 3. Delete the snapshot tree.
        var newDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
        await newDeletion.DeleteTreeAsync();

        // 4. Restore the original registry entry (or clear overrides if none existed).
        await registry.UpdateAsync(TreeId, state.State.OldRegistryEntry ?? new TreeRegistryEntry());

        // 5. Clear resize state.
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.SnapshotTreeId = null;
        state.State.OldPhysicalTreeId = null;
        state.State.OldRegistryEntry = null;
        await state.WriteStateAsync();
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
