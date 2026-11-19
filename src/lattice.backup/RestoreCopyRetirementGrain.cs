using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IRestoreCopyRetirementGrain"/> (issue #6103). Each pass
/// re-plans the tree from the registry (<see cref="RestoreCopyPlanner"/>), ends a
/// revert window that has elapsed, and retires at most one copy under the tree's
/// alias reservation (<see cref="RestoreCopyRetirement"/>), so an estate that
/// accumulated many unreachable copies is drained one shard-marking burst at a
/// time rather than all at once. A failed pass backs off exponentially, bounded by
/// <c>LatticeBackupOptions.RestoreCopyRetirementMaxBackoff</c>; another alias
/// operation holding the reservation defers the pass without counting as a
/// failure. The pass reminder exists only while a pass is owed.
/// </summary>
internal sealed class RestoreCopyRetirementGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeBackupOptions> optionsMonitor,
    ILogger<RestoreCopyRetirementGrain> logger,
    [PersistentState("restore-copy-retirement", LatticeOptions.StorageProviderName)]
    IPersistentState<RestoreCopyRetirementState> state)
    : IGrainBase, IRemindable, IRestoreCopyRetirementGrain
{
    private const string ReminderName = "restore-copy-retirement";

    /// <summary>Orleans refuses a reminder period below a minute.</summary>
    private static readonly TimeSpan MinimumReminderPeriod = TimeSpan.FromMinutes(1);

    /// <summary>The longest due time a reminder accepts.</summary>
    private static readonly TimeSpan MaximumReminderDue = TimeSpan.FromDays(49);

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString()!;

    private LatticeBackupOptions Options => optionsMonitor.CurrentValue;

    /// <inheritdoc />
    public async Task ScheduleAsync(TimeSpan delay)
    {
        EnsureInternalOrigin();
        if (!state.State.Pending)
        {
            state.State.Pending = true;
            await state.WriteStateAsync();
        }

        await ArmAsync(delay);
    }

    /// <inheritdoc />
    public Task<RestoreCopyPassOutcome> RunPassAsync()
    {
        EnsureInternalOrigin();
        return RunPassGuardedAsync();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == ReminderName)
        {
            await RunPassGuardedAsync();
        }
    }

    private async Task<RestoreCopyPassOutcome> RunPassGuardedAsync()
    {
        if (!state.State.Pending)
        {
            await DisarmAsync();
            return RestoreCopyPassOutcome.Idle;
        }

        var now = DateTimeOffset.UtcNow;
        if (state.State.NextAttemptUtc is { } next && next > now)
        {
            return RestoreCopyPassOutcome.Deferred;
        }

        try
        {
            var outcome = await RunPassCoreAsync(now);
            if (state.State.ConsecutiveFailures != 0 || state.State.NextAttemptUtc is not null)
            {
                state.State.ConsecutiveFailures = 0;
                state.State.NextAttemptUtc = null;
                await state.WriteStateAsync();
            }

            return outcome;
        }
        catch (LatticeAliasOperationInProgressException ex)
        {
            // Another alias change of the tree is in flight: not a failure, so the
            // next pass simply tries again.
            logger.LogDebug(
                "Restore-copy retirement of tree {TreeId} deferred: alias operation {Holder} holds its reservation.",
                TreeId, ex.HolderOperationId);
            return RestoreCopyPassOutcome.Deferred;
        }
        catch (Exception ex)
        {
            var failures = state.State.ConsecutiveFailures + 1;
            var backoff = Backoff(failures);
            logger.LogWarning(
                ex,
                "Restore-copy retirement pass {Failures} of tree {TreeId} failed; retrying in {Backoff}.",
                failures, TreeId, backoff);
            state.State.ConsecutiveFailures = failures;
            state.State.NextAttemptUtc = now + backoff;
            await state.WriteStateAsync();
            return RestoreCopyPassOutcome.Deferred;
        }
    }

    private async Task<RestoreCopyPassOutcome> RunPassCoreAsync(DateTimeOffset now)
    {
        // A deleted tree's copies follow the tree's own deletion; there is nothing
        // a revert could reach, and no alias change of it is admitted.
        if (await grainFactory.GetGrain<BPlusTree.ITreeDeletionGrain>(TreeId).IsDeletedAsync())
        {
            return await CompleteAsync();
        }

        LatticeRestoreCopyAudit plan;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            plan = await RestoreCopyPlanner.PlanAsync(grainFactory, TreeId);
        }

        var window = Options.RestoreRevertRetention;
        var openRevert = plan.RevertTargetPhysicalTreeId is not null || plan.Hold == LatticeRestoreCopyHold.UnknownRevertTarget;
        TimeSpan? untilFinal = null;
        if (openRevert && window is { } retention)
        {
            var committedAt = plan.RevertCommittedAtUtc ?? await ObserveUnknownRevertAsync(plan.AliasTargetPhysicalTreeId, now);
            var finalAt = committedAt + retention;
            if (finalAt <= now)
            {
                await RestoreCopyRetirement.FinalizeAsync(
                    grainFactory, TreeId, plan.AliasTargetPhysicalTreeId,
                    RestoreCopyRetirement.ReservationPrefix + "window:" + plan.AliasTargetPhysicalTreeId);
                logger.LogInformation(
                    "The shadow-cutover restore of tree {TreeId} onto {ShadowTreeId} is final: its revert window of {Window} elapsed.",
                    TreeId, plan.AliasTargetPhysicalTreeId, retention);
                using (LatticeAccessGateContext.EnterSystemOrigin())
                {
                    plan = await RestoreCopyPlanner.PlanAsync(grainFactory, TreeId);
                }
            }
            else
            {
                untilFinal = finalAt - now;
            }
        }

        if (plan.RetirablePhysicalTreeIds.Count > 0)
        {
            var copy = plan.RetirablePhysicalTreeIds[0];
            if (await RestoreCopyRetirement.RetireAsync(grainFactory, TreeId, copy))
            {
                logger.LogInformation(
                    "Retired restore copy {PhysicalTreeId} of tree {TreeId}: no revert can reach it; it is purged after the soft-delete window.",
                    copy, TreeId);
            }

            await ArmAsync(Options.RestoreCopyRetirementPassInterval);
            return RestoreCopyPassOutcome.Retired;
        }

        if (untilFinal is { } wait)
        {
            await ArmAsync(wait);
            return RestoreCopyPassOutcome.Waiting;
        }

        return await CompleteAsync();
    }

    private async Task<RestoreCopyPassOutcome> CompleteAsync()
    {
        state.State.Pending = false;
        state.State.UnknownRevertShadow = null;
        state.State.UnknownRevertObservedAtUtc = null;
        await state.WriteStateAsync();
        await DisarmAsync();
        return RestoreCopyPassOutcome.Completed;
    }

    /// <summary>
    /// The time a revertible restore whose commit time Lattice never recorded was
    /// first seen by a pass, recording it now when it is new.
    /// </summary>
    private async Task<DateTimeOffset> ObserveUnknownRevertAsync(string shadow, DateTimeOffset now)
    {
        if (string.Equals(state.State.UnknownRevertShadow, shadow, StringComparison.Ordinal)
            && state.State.UnknownRevertObservedAtUtc is { } observed)
        {
            return observed;
        }

        state.State.UnknownRevertShadow = shadow;
        state.State.UnknownRevertObservedAtUtc = now;
        await state.WriteStateAsync();
        return now;
    }

    private TimeSpan Backoff(int failures)
    {
        var interval = Options.RestoreCopyRetirementPassInterval;
        var max = Options.RestoreCopyRetirementMaxBackoff;
        var shift = Math.Min(failures - 1, 20);
        var ticks = interval.Ticks * (1L << shift);
        return ticks <= 0 || ticks > max.Ticks ? max : TimeSpan.FromTicks(ticks);
    }

    private Task ArmAsync(TimeSpan delay)
    {
        var due = delay < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : delay > MaximumReminderDue ? MaximumReminderDue : delay;
        var interval = Options.RestoreCopyRetirementPassInterval;
        var period = interval < MinimumReminderPeriod ? MinimumReminderPeriod : interval;
        return BackupReminderResilience.RunWithRetryAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(context.GrainId, ReminderName, due, period),
            logger, nameof(reminderRegistry.RegisterOrUpdateReminder), ReminderName, TreeId);
    }

    private async Task DisarmAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            // A reminder that survives finds nothing pending and disarms again.
            logger.LogWarning(ex, "Failed to unregister the restore-copy retirement reminder of tree {TreeId}.", TreeId);
        }
    }

    private void EnsureInternalOrigin() =>
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);
}
