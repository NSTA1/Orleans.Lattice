using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class TreeDeletionGrain
{
    private const string LogicalReminderName = "logical-tree-deletion";

    public async Task EnsureAliasWritableAsync()
    {
        if (await IsDeletedAsync() || state.State.LocalDeleteTargetPinned)
            throw Refuse($"Cannot change an alias involving deleted tree '{TreeId}'; recover it first.");
    }

    public async Task BeginAliasChangeAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        EnsureLifecycleOrigin();
        if (await IsDeletedAsync() || state.State.LocalDeleteTargetPinned)
            throw Refuse($"Tree '{TreeId}' is logically deleted; recover it before changing its alias.");
        if (state.State.AliasOperationId is { } active && active != operationId)
            throw Refuse($"Tree '{TreeId}' has alias operation '{active}' in progress; retry after it completes.");
        if (state.State.AliasOperationId == operationId) return;
        state.State.AliasOperationId = operationId;
        try { await state.WriteStateAsync(); }
        catch { state.State.AliasOperationId = null; throw; }
    }

    public async Task EndAliasChangeAsync(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        EnsureLifecycleOrigin();
        if (state.State.AliasOperationId != operationId) return;
        state.State.AliasOperationId = null;
        try { await state.WriteStateAsync(); }
        catch { state.State.AliasOperationId = operationId; throw; }
    }

    public async Task DeleteDelegatedAsync()
    {
        EnsureLifecycleOrigin();
        var delegated = state.State.Delegated;
        var suppressed = state.State.SuppressLifecycleEvents;
        state.State.Delegated = true;
        state.State.SuppressLifecycleEvents = true;
        try { await state.WriteStateAsync(); }
        catch
        {
            state.State.Delegated = delegated;
            state.State.SuppressLifecycleEvents = suppressed;
            throw;
        }
        await SoftDeleteAsync(retainsRegistryEntry: false);
    }

    public async Task DeleteTreeAsync()
    {
        EnsureLifecycleOrigin();
        if (state.State.AliasOperationId is { } operation)
            throw Refuse($"Cannot delete tree '{TreeId}': alias operation '{operation}' is in progress.");
        if (state.State.LocalDeleteTargetPinned)
        {
            if (state.State.DeletePending)
            {
                state.State.DeletePending = false;
                try { await state.WriteStateAsync(); }
                catch { state.State.DeletePending = true; throw; }
            }
            await SoftDeleteAsync(retainsRegistryEntry: false);
            return;
        }
        if (state.State.LogicalPhysicalTreeId is null)
        {
            if (!state.State.RetainsRegistryEntry && state.State.IsDeleted) return;
            // Published before the exclusive registry read: earlier alias writers
            // drain before validation, and subsequent writers see this fence.
            state.State.DeletePending = true;
            try { await state.WriteStateAsync(); }
            catch { state.State.DeletePending = false; throw; }

            try
            {
                var registry = grainFactory.GetLatticeRegistry();
                var incoming = await registry.GetAliasesTargetingAsync(TreeId);
                if (incoming.Count != 0)
                    throw Refuse($"Cannot delete physical tree '{TreeId}' while another tree aliases it.");
                var physical = await registry.ResolveAsync(TreeId);
                if (physical == TreeId)
                {
                    if (state.State.RetainsRegistryEntry)
                    {
                        if (!state.State.PurgeComplete)
                            throw Refuse($"Tree '{TreeId}' resolves to its retired physical copy; undo the resize or restore the live alias first.");
                        await ResetPurgedRetirementAsync();
                    }
                    state.State.LocalDeleteTargetPinned = true;
                    try { await state.WriteStateAsync(); }
                    catch { state.State.LocalDeleteTargetPinned = false; throw; }
                    await SoftDeleteAsync(retainsRegistryEntry: false);
                }
                else
                {
                    await ValidateOwnedTargetAsync(physical);
                    state.State.LogicalPhysicalTreeId = physical;
                    state.State.LogicalDeletedAtUtc = DateTimeOffset.UtcNow;
                    state.State.LogicalDeleteComplete = false;
                    state.State.LogicalPurgeComplete = false;
                    state.State.LogicalPurgeInProgress = false;
                    try { await state.WriteStateAsync(); }
                    catch
                    {
                        state.State.LogicalPhysicalTreeId = null;
                        state.State.LogicalDeletedAtUtc = null;
                        throw;
                    }
                }
            }
            finally
            {
                state.State.DeletePending = false;
                await state.WriteStateAsync();
            }
        }
        if (state.State.LogicalPhysicalTreeId is null || state.State.LogicalDeleteComplete) return;

        // Anchor first. A failed mark/compaction call is retried by DeleteTreeAsync,
        // and a lost response cannot leave an unanchored durable deletion.
        await EnsureLogicalReminderAsync();
        await grainFactory.GetGrain<ITreeDeletionGrain>(state.State.LogicalPhysicalTreeId)
            .DeleteDelegatedAsync();
        await grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId).UnregisterReminderAsync();
        state.State.LogicalDeleteComplete = true;
        state.State.DeletePending = false;
        try { await state.WriteStateAsync(); }
        catch { state.State.LogicalDeleteComplete = false; throw; }
        await PublishLogicalLifecycleEventAsync(LatticeTreeEventKind.TreeDeleted);
    }

    private async Task ValidateOwnedTargetAsync(string physical)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var entry = await registry.GetEntryAsync(physical);
        if (!string.Equals(entry?.DerivedFrom, TreeId, StringComparison.Ordinal))
            throw Refuse($"Cannot delete alias target '{physical}': it is not derived from tree '{TreeId}'.");
        foreach (var alias in await registry.GetAliasesTargetingAsync(physical))
            if (alias != TreeId)
                throw Refuse($"Cannot delete alias target '{physical}': tree '{alias}' also aliases it.");
    }

    private async Task ResetPurgedRetirementAsync()
    {
        var snapshot = (state.State.IsDeleted, state.State.DeletedAtUtc,
            state.State.RetainsRegistryEntry, state.State.PurgeComplete, state.State.SuppressLifecycleEvents);
        state.State.IsDeleted = false;
        state.State.DeletedAtUtc = null;
        state.State.RetainsRegistryEntry = false;
        state.State.PurgeComplete = false;
        state.State.SuppressLifecycleEvents = false;
        try { await state.WriteStateAsync(); }
        catch
        {
            (state.State.IsDeleted, state.State.DeletedAtUtc,
                state.State.RetainsRegistryEntry, state.State.PurgeComplete, state.State.SuppressLifecycleEvents) = snapshot;
            throw;
        }
    }

    private async Task RecoverLogicalAsync()
    {
        EnsureLifecycleOrigin();
        if (state.State.LogicalPurgeComplete || state.State.LogicalPurgeInProgress)
            throw Refuse($"Cannot recover tree '{TreeId}': its purge has started or completed.");
        var physical = state.State.LogicalPhysicalTreeId!;
        // A failed recovery may already have unmarked the target. A subsequent
        // delete or purge must re-drive the marks rather than trust completion
        // from before that recovery attempt.
        var wasComplete = state.State.LogicalDeleteComplete;
        state.State.LogicalDeleteComplete = false;
        try { await state.WriteStateAsync(); }
        catch { state.State.LogicalDeleteComplete = wasComplete; throw; }
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(physical);
        if (await deletion.IsPhysicalDeletedAsync())
            await deletion.RecoverPhysicalAsync();
        await grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId).EnsureReminderAsync();
        await RemoveLogicalReminderAsync();
        var deletedAt = state.State.LogicalDeletedAtUtc;
        var pending = state.State.DeletePending;
        state.State.LogicalPhysicalTreeId = null;
        state.State.LogicalDeletedAtUtc = null;
        state.State.DeletePending = false;
        try { await state.WriteStateAsync(); }
        catch
        {
            state.State.LogicalPhysicalTreeId = physical;
            state.State.LogicalDeletedAtUtc = deletedAt;
            state.State.DeletePending = pending;
            await ReanchorLogicalReminderAfterFailureAsync();
            throw;
        }
        await PublishLogicalLifecycleEventAsync(LatticeTreeEventKind.TreeRecovered);
    }

    private async Task PurgeLogicalAsync()
    {
        EnsureLifecycleOrigin();
        if (state.State.LogicalPurgeComplete)
            throw Refuse($"Tree '{TreeId}' has already been fully purged.");
        if (!state.State.LogicalDeleteComplete) await DeleteTreeAsync();
        var physical = state.State.LogicalPhysicalTreeId!;
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(physical);
        // A prior attempt may already have removed the physical registry entry.
        // Its durable completion is the only evidence that permits that absence.
        if (!(await deletion.GetDeletionStatusAsync()).PurgeComplete)
            await ValidateOwnedTargetAsync(physical);
        var wasPurging = state.State.LogicalPurgeInProgress;
        state.State.LogicalPurgeInProgress = true;
        try { await state.WriteStateAsync(); }
        catch { state.State.LogicalPurgeInProgress = wasPurging; throw; }

        await deletion.PurgePhysicalAsync();
        if (state.State.RetainsRegistryEntry && state.State.IsDeleted && !state.State.PurgeComplete)
            await PurgePhysicalAsync();
        await grainFactory.GetLatticeRegistry().UnregisterAsync(TreeId);
        await RemoveLogicalReminderAsync();
        state.State.LogicalPurgeInProgress = false;
        state.State.LogicalPurgeComplete = true;
        try { await state.WriteStateAsync(); }
        catch
        {
            state.State.LogicalPurgeInProgress = true;
            state.State.LogicalPurgeComplete = false;
            await ReanchorLogicalReminderAfterFailureAsync();
            throw;
        }
        await PublishLogicalLifecycleEventAsync(LatticeTreeEventKind.TreePurged);
    }

    private Task EnsureLogicalReminderAsync()
    {
        var period = ClampPeriod(Options.SoftDeleteDuration);
        return ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(context.GrainId, LogicalReminderName, period, period),
            ReminderRegistrationBackoff);
    }

    private async Task ReanchorLogicalReminderAfterFailureAsync()
    {
        try { await EnsureLogicalReminderAsync(); }
        catch (Exception fault)
        {
            logger.LogError(fault,
                "Tree {TreeId}: failed to restore logical purge reminder after a state-write failure; retry the lifecycle operation.",
                TreeId);
        }
    }

    private async Task RemoveLogicalReminderAsync()
    {
        var reminder = await reminderRegistry.GetReminder(context.GrainId, LogicalReminderName);
        if (reminder is not null) await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
    }

    private void EnsureLifecycleOrigin() =>
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

    private InvalidOperationException Refuse(string message)
    {
        logger.LogWarning("Tree lifecycle operation refused: {Reason}", message);
        return new InvalidOperationException(message);
    }
}
