using Orleans.Lattice.Api.Backup;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The schedule-management concern: the modal that registers, re-times, and
/// removes a scope's recurring capture.
/// </summary>
public partial class BackupsPanel
{
    // Schedule-management modal state: the row (single-scope) whose recurring
    // schedule is being edited or removed, the loaded scope status, and the
    // per-kind interval editors prefilled from the current runtime cadence.
    private BackupRow? _pendingSchedule;
    private BackupScopeStatus? _scheduleStatus;
    private bool _scheduleLoading;
    private int _editFullHours;
    private int _editFullMinutes;
    private int _editIncHours;
    private int _editIncMinutes;

    /// <summary>The row whose schedule is being managed, or <see langword="null"/>.</summary>
    internal BackupRow? PendingSchedule => _pendingSchedule;

    /// <summary>The loaded schedule status for that row's scope, or <see langword="null"/>.</summary>
    internal BackupScopeStatus? ScheduleStatus => _scheduleStatus;

    /// <summary>Whether the schedule status is still loading.</summary>
    internal bool ScheduleLoading => _scheduleLoading;

    /// <summary>The hours component of the edited full-backup cadence.</summary>
    internal int EditFullHours => _editFullHours;

    /// <summary>The minutes component of the edited full-backup cadence.</summary>
    internal int EditFullMinutes => _editFullMinutes;

    /// <summary>The hours component of the edited incremental-backup cadence.</summary>
    internal int EditIncHours => _editIncHours;

    /// <summary>The minutes component of the edited incremental-backup cadence.</summary>
    internal int EditIncMinutes => _editIncMinutes;

    /// <summary>The edited full-backup cadence.</summary>
    internal TimeSpan EditFullInterval => new(hours: _editFullHours, minutes: _editFullMinutes, seconds: 0);

    /// <summary>The edited incremental-backup cadence.</summary>
    internal TimeSpan EditIncrementalInterval => new(hours: _editIncHours, minutes: _editIncMinutes, seconds: 0);

    /// <summary>Sets the hours component of the edited full-backup cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 168.</param>
    internal void SetEditFullHours(object? value) => _editFullHours = ParseInterval(value, 0, 168);

    /// <summary>Sets the minutes component of the edited full-backup cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 59.</param>
    internal void SetEditFullMinutes(object? value) => _editFullMinutes = ParseInterval(value, 0, 59);

    /// <summary>Sets the hours component of the edited incremental-backup cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 168.</param>
    internal void SetEditIncHours(object? value) => _editIncHours = ParseInterval(value, 0, 168);

    /// <summary>Sets the minutes component of the edited incremental-backup cadence.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 59.</param>
    internal void SetEditIncMinutes(object? value) => _editIncMinutes = ParseInterval(value, 0, 59);

    /// <summary>
    /// Opens the schedule-management modal for a single-scope row, loading the
    /// scope's current schedule status and prefilling each interval editor from
    /// the registered runtime cadence (defaulting to one hour when unset).
    /// </summary>
    /// <param name="row">The single-scope row whose schedule to manage.</param>
    internal async Task OpenScheduleAsync(BackupRow row)
    {
        _pendingSchedule = row;
        _scheduleStatus = null;
        _scheduleLoading = true;
        NotifyStateChanged();

        var scope = row.Members[0].Scope;
        BackupScopeStatus? status = null;
        try
        {
            status = await Reader.GetScheduleStatusAsync(scope);
        }
        finally
        {
            _scheduleStatus = status;
            PrefillScheduleEditors(status);
            _scheduleLoading = false;
            NotifyStateChanged();
        }
    }

    /// <summary>Closes the schedule-management modal.</summary>
    internal void CloseSchedule()
    {
        _pendingSchedule = null;
        _scheduleStatus = null;
        NotifyStateChanged();
    }

    /// <summary>
    /// Registers or updates the schedule of the chosen kind at the edited
    /// interval, then reloads the scope status so the modal reflects the new
    /// cadence.
    /// </summary>
    /// <param name="incremental">Whether the incremental schedule is being saved.</param>
    internal async Task SaveScheduleAsync(bool incremental)
    {
        var row = _pendingSchedule;
        if (row is null)
        {
            return;
        }

        var scope = row.Members[0].Scope;
        var interval = incremental ? EditIncrementalInterval : EditFullInterval;
        if (interval <= TimeSpan.Zero)
        {
            return;
        }

        BeginBusy();
        try
        {
            _lastResult = await Reader.ScheduleAsync(scope, incremental, interval);
            _scheduleStatus = await Reader.GetScheduleStatusAsync(scope);
            PrefillScheduleEditors(_scheduleStatus);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Removes the schedule of the chosen kind, then reloads the scope status
    /// and closes the modal when no schedule remains.
    /// </summary>
    /// <param name="incremental">Whether the incremental schedule is being removed.</param>
    internal async Task RemoveScheduleAsync(bool incremental)
    {
        var row = _pendingSchedule;
        if (row is null)
        {
            return;
        }

        var scope = row.Members[0].Scope;
        BeginBusy();
        try
        {
            _lastResult = await Reader.UnscheduleAsync(scope, incremental);
            _scheduleStatus = await Reader.GetScheduleStatusAsync(scope);
            PrefillScheduleEditors(_scheduleStatus);
        }
        finally
        {
            EndBusy();
        }

        if (_scheduleStatus is null
            || (!_scheduleStatus.FullScheduleRegistered && !_scheduleStatus.IncrementalScheduleRegistered))
        {
            CloseSchedule();
        }
    }

    private void PrefillScheduleEditors(BackupScopeStatus? status)
    {
        var full = status?.RuntimeFullBackupInterval ?? TimeSpan.FromHours(1);
        _editFullHours = (int)full.TotalHours;
        _editFullMinutes = full.Minutes;

        var incremental = status?.RuntimeIncrementalBackupInterval ?? TimeSpan.FromHours(1);
        _editIncHours = (int)incremental.TotalHours;
        _editIncMinutes = incremental.Minutes;
    }
}
