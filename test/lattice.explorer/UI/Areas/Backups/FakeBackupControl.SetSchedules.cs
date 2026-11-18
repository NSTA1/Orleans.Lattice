using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The backup-set schedule half of the scripted facade (#6067): an in-memory
/// schedule store, served by default, with every verb overridable by a delegate.
/// Every call is recorded in <see cref="FakeBackupControl.Calls"/>.
/// </summary>
internal sealed partial class FakeBackupControl
{
    /// <summary>The registered backup-set schedules by name.</summary>
    public SortedDictionary<string, BackupSetScheduleStatus> SetSchedules { get; } = new(StringComparer.Ordinal);

    /// <summary>The listing; reads <see cref="SetSchedules"/> by default.</summary>
    public Func<Task<IReadOnlyList<BackupSetScheduleStatus>>>? ListSetSchedules { get; set; }

    /// <summary>The schedule verb; stores a scheduled status by default.</summary>
    public Func<LatticeBackupSetScheduleRequest, Task<BackupSetScheduleStatus>>? ScheduleSet { get; set; }

    /// <summary>The pause verb; marks a stored schedule paused by default.</summary>
    public Func<string, Task<bool>>? PauseSet { get; set; }

    /// <summary>The remove verb; forgets a stored schedule by default.</summary>
    public Func<string, bool, Task<bool>>? RemoveSet { get; set; }

    /// <summary>The run-now verb; true for a stored schedule, <see cref="KeyNotFoundException"/> otherwise, by default.</summary>
    public Func<string, Task<bool>>? RunSetNow { get; set; }

    /// <summary>A scheduled backup-set status for tests.</summary>
    /// <param name="request">The schedule's request.</param>
    /// <param name="scheduled">Whether it is scheduled rather than paused.</param>
    /// <param name="runs">Its recent runs.</param>
    /// <param name="sets">Its retained sets.</param>
    public static BackupSetScheduleStatus SetStatus(
        LatticeBackupSetScheduleRequest request,
        bool scheduled = true,
        IReadOnlyList<BackupSetScheduleRun>? runs = null,
        IReadOnlyList<ScheduledBackupSet>? sets = null) =>
        new(
            request.Name,
            request,
            request.Membership == BackupSetMembershipMode.Explicit ? request.Scopes : [BackupScopeSelector.WholeTree("orders")],
            scheduled,
            scheduled ? new DateTimeOffset(2026, 10, 1, 2, 0, 0, TimeSpan.Zero) : null,
            isRunning: false,
            new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero),
            lastRunUtc: null,
            lastSuccessUtc: null,
            BackupScopeRunOutcome.None,
            lastFailureReason: null,
            consecutiveFailures: 0,
            runs ?? [],
            sets ?? []);

    /// <inheritdoc />
    public Task<BackupSetScheduleStatus> ScheduleBackupSetAsync(LatticeBackupSetScheduleRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ScheduleBackupSetAsync), request));
        if (ScheduleSet is { } schedule)
        {
            return schedule(request);
        }

        var status = SetStatus(request);
        SetSchedules[request.Name] = status;
        return Task.FromResult(status);
    }

    /// <inheritdoc />
    public Task<bool> CancelBackupSetScheduleAsync(string name, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CancelBackupSetScheduleAsync), name));
        if (PauseSet is { } pause)
        {
            return pause(name);
        }

        if (!SetSchedules.TryGetValue(name, out var status))
        {
            return Task.FromResult(false);
        }

        SetSchedules[name] = status with { IsScheduled = false, NextRunUtc = null };
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<bool> RemoveBackupSetScheduleAsync(string name, bool deleteRetainedSets, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(RemoveBackupSetScheduleAsync), (name, deleteRetainedSets)));
        return RemoveSet?.Invoke(name, deleteRetainedSets) ?? Task.FromResult(SetSchedules.Remove(name));
    }

    /// <inheritdoc />
    public Task<BackupSetScheduleStatus?> GetBackupSetScheduleStatusAsync(string name, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(GetBackupSetScheduleStatusAsync), name));
        return Task.FromResult(SetSchedules.TryGetValue(name, out var status) ? status : null);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<BackupSetScheduleStatus>> ListBackupSetSchedulesAsync(CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ListBackupSetSchedulesAsync), null));
        return ListSetSchedules?.Invoke()
            ?? Task.FromResult<IReadOnlyList<BackupSetScheduleStatus>>([.. SetSchedules.Values]);
    }

    /// <inheritdoc />
    public Task<bool> RunBackupSetScheduleNowAsync(string name, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(RunBackupSetScheduleNowAsync), name));
        if (RunSetNow is { } run)
        {
            return run(name);
        }

        return SetSchedules.ContainsKey(name)
            ? Task.FromResult(true)
            : Task.FromException<bool>(new KeyNotFoundException(name));
    }
}
