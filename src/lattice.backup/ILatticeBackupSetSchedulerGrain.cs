using Orleans.Concurrency;

namespace Orleans.Lattice.Backup;

/// <summary>
/// One backup-set schedule (issue #6067), keyed by
/// <see cref="BackupSetScheduleKey.For"/> of the schedule's effective name. A
/// durable reminder starts one run per interval; a run resolves the schedule's
/// member trees, captures one backup set over them (cross-tree consistent by
/// default, retrying a fence that could not be established a bounded number of
/// times), records the set's exact membership, and applies the schedule's
/// retention to whole sets. A run never overlaps another: a tick, or a
/// run-now request, that arrives while a run is in flight is coalesced into it.
/// </summary>
/// <remarks>
/// Performs no authorization of its own. The API facade authorizes the caller
/// over the schedule's trees (or, for a schedule that captures trees created
/// later, over the tenant-wide or cluster-wide scope) before it reaches this
/// grain, and a run executes system-origin because a reminder has no caller.
/// </remarks>
[Alias(BackupTypeAliases.ILatticeBackupSetSchedulerGrain)]
internal interface ILatticeBackupSetSchedulerGrain : IGrainWithStringKey
{
    /// <summary>
    /// Registers or updates the schedule and returns its status. Re-registering
    /// with an unchanged interval keeps the reminder - and so the timing of the
    /// next run - as it is.
    /// </summary>
    /// <param name="request">The effective (tenant-composed) configuration.</param>
    /// <param name="target">What the facade resolved for the caller.</param>
    Task<BackupSetScheduleStatus> ScheduleAsync(LatticeBackupSetScheduleRequest request, BackupSetScheduleTarget target);

    /// <summary>
    /// Pauses the schedule: unregisters its reminder and keeps its configuration
    /// and retained sets, which stay restorable and held from per-tree retention.
    /// Returns <see langword="false"/> when the schedule does not exist.
    /// </summary>
    Task<bool> PauseAsync();

    /// <summary>
    /// Removes the schedule: unregisters its reminder, lifts its retention holds,
    /// removes it from the schedule index and clears its state. With
    /// <paramref name="deleteRetainedSets"/> set, first deletes the backups of its
    /// retained sets that no other schedule holds and no other capture has since
    /// taken over. Returns <see langword="false"/> when the schedule does not exist.
    /// </summary>
    /// <param name="deleteRetainedSets">Whether to delete the retained sets' backups.</param>
    Task<bool> RemoveAsync(bool deleteRetainedSets);

    /// <summary>Returns the schedule's status, or <see langword="null"/> when it does not exist.</summary>
    [AlwaysInterleave]
    Task<BackupSetScheduleStatus?> GetStatusAsync();

    /// <summary>
    /// Starts one run now, without waiting for it. Returns <see langword="true"/>
    /// when a run was started and <see langword="false"/> when one was already in
    /// flight (the request is coalesced into it). Throws
    /// <see cref="KeyNotFoundException"/> when the schedule does not exist.
    /// </summary>
    [AlwaysInterleave]
    Task<bool> RunNowAsync();

    /// <summary>
    /// Runs one cycle and waits for it, returning the run's record, or
    /// <see langword="null"/> when a run was already in flight. The same work a
    /// reminder tick starts; exposed so it can be driven directly.
    /// </summary>
    [AlwaysInterleave]
    Task<BackupSetScheduleRun?> RunCycleAsync();

    /// <summary>Returns the retained set with the id <paramref name="setId"/>, or <see langword="null"/>.</summary>
    /// <param name="setId">The set id.</param>
    [AlwaysInterleave]
    Task<ScheduledBackupSet?> GetSetAsync(string setId);
}
