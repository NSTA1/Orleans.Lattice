using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The accept-then-poll purge (issue #3941).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="PurgeNowAsync"/> walks every shard inside one grain call, so the
/// caller's response timeout - not the work - decided when a large purge
/// stopped: the call was abandoned after the shard in flight, the tree was left
/// part purged, and the status read queued behind the walk and reported nothing
/// in progress. <see cref="BeginPurgeAsync"/> instead persists the purge as in
/// progress and hands the walk to the grain timer the reminder-driven purge
/// already uses, anchored by its keepalive reminder, then returns.
/// </para>
/// <para>
/// <see cref="GetDeletionStatusAsync"/> is interleaved so it answers while a
/// timer tick holds the activation in a shard's purge. It reads
/// <see cref="_durable"/>, which moves only once a write has reached storage:
/// every mutation here changes the in-memory state first, awaits the write, and
/// reverts on failure, so an interleaved read of the live state could report a
/// transition that is then rolled back.
/// </para>
/// </remarks>
internal sealed partial class TreeDeletionGrain
{
    private const string LogicalPurgeKeepaliveReminderName = "logical-purge-keepalive";

    /// <summary>
    /// The pause between two checks, by the logical owner of an aliased tree,
    /// of whether its pinned copy's purge has finished.
    /// </summary>
    internal static readonly TimeSpan LogicalPurgePollPeriod = TimeSpan.FromMilliseconds(250);

    private IGrainTimer? _logicalPurgeTimer;

    private DurableDeletion? _durable;

    /// <summary>The deletion fields the interleaved status read reports, as last persisted.</summary>
    private readonly record struct DurableDeletion(
        bool IsDeleted,
        DateTimeOffset? DeletedAtUtc,
        bool RetainsRegistryEntry,
        bool PurgeInProgress,
        bool PurgeComplete,
        int NextShardIndex,
        int PurgeShardCount,
        string? LogicalPhysicalTreeId,
        DateTimeOffset? LogicalDeletedAtUtc,
        bool LogicalPurgeInProgress,
        bool LogicalPurgeComplete,
        bool RegistryUnregisterPending,
        long DeletionEpoch);

    private DurableDeletion Durable => _durable ??= CaptureDurable();

    private DurableDeletion CaptureDurable()
    {
        var s = state.State;
        return new DurableDeletion(
            s.IsDeleted, s.DeletedAtUtc, s.RetainsRegistryEntry, s.PurgeInProgress, s.PurgeComplete,
            s.NextShardIndex, s.PurgeShardCount, s.LogicalPhysicalTreeId, s.LogicalDeletedAtUtc,
            s.LogicalPurgeInProgress, s.LogicalPurgeComplete, s.RegistryUnregisterPending, s.DeletionEpoch);
    }

    /// <inheritdoc />
    Task IGrainBase.OnActivateAsync(CancellationToken token)
    {
        _durable = CaptureDurable();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Persists <see cref="Orleans.Lattice.BPlusTree.State.TreeDeletionState"/> and, only once the write has
    /// succeeded, publishes what was written to the interleaved status read.
    /// </summary>
    private async Task PersistAsync()
    {
        var written = CaptureDurable();
        await state.WriteStateAsync();
        _durable = written;
    }

    /// <inheritdoc />
    public async Task<TreeDeletionSnapshot> GetDeletionStatusAsync()
    {
        // A pure read: no internal-origin assertion (mirrors IsDeletedAsync), so
        // the diagnostics facade can dial it directly. The recovery deadline is
        // derived from the persisted delete time and the tree's configured
        // soft-delete duration; it is null while the tree is live. A purged
        // tree whose id was registered again reads as the live tree it now is,
        // so this never disagrees with TreeExistsAsync.
        if (await IsReusedAfterPurgeAsync())
            return new TreeDeletionSnapshot();
        var d = Durable;
        if (d.LogicalPhysicalTreeId is { } physical)
        {
            var (done, total) = (0, 0);
            if (d.LogicalPurgeInProgress || d.LogicalPurgeComplete)
            {
                var target = await grainFactory.GetGrain<ITreeDeletionGrain>(physical).GetDeletionStatusAsync();
                (done, total) = (target.PurgedShardCount, target.PurgeShardCount);
                if (d.RetainsRegistryEntry && d.IsDeleted)
                {
                    var (localDone, localTotal) = PhysicalProgress(d);
                    (done, total) = (done + localDone, total + localTotal);
                }
            }

            return new TreeDeletionSnapshot
            {
                IsDeleted = true,
                DeletedAtUtc = d.LogicalDeletedAtUtc,
                RecoveryDeadlineUtc = d.LogicalDeletedAtUtc + Options.SoftDeleteDuration,
                PurgeInProgress = d.LogicalPurgeInProgress,
                PurgeComplete = d.LogicalPurgeComplete,
                PurgedShardCount = done,
                PurgeShardCount = total,
                DeletionEpoch = d.DeletionEpoch,
            };
        }

        var retired = d.RetainsRegistryEntry;
        var deletedAt = retired ? null : d.DeletedAtUtc;
        var (purged, shards) = retired ? (0, 0) : PhysicalProgress(d);

        // A purge that has recorded its completion but not yet removed the
        // tree's registry entry is still running (issue #4252): reporting it
        // complete here let PurgeTreeAsync return while TreeExistsAsync still
        // found the tree. The walk is done, so every shard reads as purged. The
        // owed removal is durable, so this holds across a reactivation until a
        // removal that threw is re-driven (issue #4265).
        var finalising = !retired && d.PurgeComplete && d.RegistryUnregisterPending;
        return new TreeDeletionSnapshot
        {
            IsDeleted = !retired && d.IsDeleted,
            DeletedAtUtc = deletedAt,
            RecoveryDeadlineUtc = deletedAt is { } at ? at + Options.SoftDeleteDuration : null,
            PurgeInProgress = !retired && (d.PurgeInProgress || finalising),
            PurgeComplete = !retired && d.PurgeComplete && !finalising,
            PurgedShardCount = purged,
            PurgeShardCount = shards,
            DeletionEpoch = d.DeletionEpoch,
        };
    }

    /// <summary>
    /// The local physical walk's progress as shards done and shards in total: the
    /// recorded shard index while it runs, every shard once it has completed, and
    /// nothing when no purge has started.
    /// </summary>
    private static (int Done, int Total) PhysicalProgress(DurableDeletion d)
    {
        if (d.PurgeComplete) return (d.PurgeShardCount, d.PurgeShardCount);
        if (!d.PurgeInProgress) return (0, 0);
        return (d.PurgeShardCount > 0 ? Math.Min(d.NextShardIndex, d.PurgeShardCount) : d.NextShardIndex,
            d.PurgeShardCount);
    }

    /// <inheritdoc />
    public async Task BeginPurgeAsync()
    {
        EnsureLifecycleOrigin();

        // A retry after a completed purge reports its success only while the id
        // stays unregistered: once it names a new, live tree the stale record is
        // cleared and the purge is refused as for any tree that is not deleted.
        await ClearRecordIfReusedAfterPurgeAsync();
        if (state.State.LogicalPhysicalTreeId is not null)
        {
            await BeginLogicalPurgeAsync();
            return;
        }
        if (state.State.RetainsRegistryEntry)
            throw Refuse("Cannot purge a tree that has not been deleted; its retired physical copy is separate.");
        await BeginPhysicalPurgeAsync();
    }

    /// <summary>
    /// Accepts the purge of the shards stored under this id: records it as in
    /// progress and arms the timer that walks them, then returns.
    /// </summary>
    private async Task BeginPhysicalPurgeAsync()
    {
        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot purge a tree that has not been deleted.");

        if (state.State.PurgeComplete)
        {
            // A retry after the purge finished reports that success. A delegated
            // copy re-drives its registry cleanup, as PurgePhysicalAsync does,
            // and so does a purge whose registry removal threw (issue #4265).
            if (state.State.Delegated)
            {
                await SettleRegistryEntryAsync();
                await DeregisterLeafCursorsAsync();
            }
            else if (state.State.RegistryUnregisterPending)
            {
                await FinishCompletedPurgeAsync();
            }
            return;
        }

        if (state.State.PurgeInProgress && state.State.PurgeRequested && _purgeTimer is not null)
            return;

        await StartPurgeAsync(
            startFromShard: state.State.PurgeInProgress ? state.State.NextShardIndex : 0,
            requested: true);
    }

    /// <summary>
    /// Accepts the purge of an aliased tree: records it as in progress, starts
    /// the pinned copy's purge (and a retired copy's under this id, if one is
    /// still waiting), and arms the timer that completes the logical purge once
    /// they have finished.
    /// </summary>
    private async Task BeginLogicalPurgeAsync()
    {
        if (state.State.LogicalPurgeComplete) return;
        if (!state.State.LogicalDeleteComplete) await DeleteTreeAsync();

        var physical = state.State.LogicalPhysicalTreeId!;
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(physical);

        // A prior attempt may already have removed the physical registry entry.
        // Its durable completion is the only evidence that permits that absence.
        var target = await deletion.GetDeletionStatusAsync();
        if (!target.PurgeComplete)
            await ValidateOwnedTargetAsync(physical);

        await AnnouncePurgeAsync();
        if (!state.State.LogicalPurgeInProgress)
        {
            state.State.LogicalPurgeInProgress = true;
            try { await PersistAsync(); }
            catch { state.State.LogicalPurgeInProgress = false; throw; }
        }

        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(
                context.GrainId, LogicalPurgeKeepaliveReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1)),
            ReminderRegistrationBackoff);

        // The copy's own status is interleaved, so this never waits on its walk;
        // only a copy whose purge has not been accepted yet is asked to start.
        if (!target.PurgeComplete && !target.PurgeInProgress)
            await deletion.BeginPurgeAsync();
        await StartRetiredCopyPurgeIfWaitingAsync();

        // A copy already purged by an earlier attempt needs no timer to finish.
        if (target.PurgeComplete && await TryCompleteLogicalPurgeAsync()) return;
        ArmLogicalPurgeTimer();
    }

    /// <summary>
    /// Starts the purge of the retired physical copy stored under this logical
    /// id, if the logical purge includes one that is not already being walked.
    /// </summary>
    private async Task StartRetiredCopyPurgeIfWaitingAsync()
    {
        if (!state.State.RetainsRegistryEntry || !state.State.IsDeleted || state.State.PurgeComplete) return;
        if (_purgeTimer is not null) return;
        await StartPurgeAsync(
            startFromShard: state.State.PurgeInProgress ? state.State.NextShardIndex : 0,
            requested: true);
    }

    private void ArmLogicalPurgeTimer()
    {
        _logicalPurgeTimer ??= this.RegisterGrainTimer(
            OnLogicalPurgeTickAsync,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: LogicalPurgePollPeriod));
    }

    private void StopLogicalPurgeTimer()
    {
        _logicalPurgeTimer?.Dispose();
        _logicalPurgeTimer = null;
    }

    private async Task OnLogicalPurgeTickAsync(CancellationToken cancellationToken)
    {
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        try
        {
            if (await TryCompleteLogicalPurgeAsync())
                StopLogicalPurgeTimer();
        }
        catch (Exception fault)
        {
            logger.LogWarning(fault,
                "Tree {TreeId}: completing the logical purge failed; it is retried on the next tick.",
                TreeId);
        }
    }

    /// <summary>
    /// Completes an accepted logical purge once the pinned copy's purge, and any
    /// retired copy's under this id, have finished: unregisters the logical tree
    /// and records the purge complete. Re-starts a copy's walk that is neither
    /// running nor complete.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when there is nothing left to drive; otherwise
    /// <see langword="false"/>.
    /// </returns>
    internal async Task<bool> TryCompleteLogicalPurgeAsync()
    {
        if (state.State.LogicalPhysicalTreeId is not { } physical
            || state.State.LogicalPurgeComplete
            || !state.State.LogicalPurgeInProgress)
            return true;

        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(physical);
        var target = await deletion.GetDeletionStatusAsync();
        if (!target.PurgeComplete)
        {
            if (!target.PurgeInProgress)
                await deletion.BeginPurgeAsync();
            return false;
        }

        if (state.State.RetainsRegistryEntry && state.State.IsDeleted && !state.State.PurgeComplete)
        {
            await StartRetiredCopyPurgeIfWaitingAsync();
            return false;
        }

        await grainFactory.GetLatticeRegistry().UnregisterAsync(TreeId);
        state.State.LogicalPurgeInProgress = false;
        state.State.LogicalPurgeComplete = true;
        try { await PersistAsync(); }
        catch
        {
            state.State.LogicalPurgeInProgress = true;
            state.State.LogicalPurgeComplete = false;
            throw;
        }

        // Both reminders tear themselves down on their next tick once the purge
        // is recorded complete, so a failure here only delays that.
        try
        {
            await RemoveLogicalReminderAsync();
            await RemoveLogicalPurgeKeepaliveAsync();
        }
        catch (Exception fault)
        {
            logger.LogWarning(fault,
                "Tree {TreeId}: failed to unregister a logical purge reminder; it unregisters itself on its next tick.",
                TreeId);
        }

        await PublishLogicalLifecycleEventAsync(LatticeTreeEventKind.TreePurged);
        return true;
    }

    /// <summary>
    /// Resumes driving an accepted logical purge after a reactivation, or
    /// unregisters the keepalive once there is nothing left to drive.
    /// </summary>
    private async Task OnLogicalPurgeKeepaliveAsync()
    {
        if (state.State.LogicalPhysicalTreeId is null
            || state.State.LogicalPurgeComplete
            || !state.State.LogicalPurgeInProgress)
        {
            await RemoveLogicalPurgeKeepaliveAsync();
            return;
        }

        ArmLogicalPurgeTimer();
    }

    private async Task RemoveLogicalPurgeKeepaliveAsync()
    {
        var reminder = await reminderRegistry.GetReminder(context.GrainId, LogicalPurgeKeepaliveReminderName);
        if (reminder is not null) await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
    }
}
