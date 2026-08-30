using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The backup-health concern: availability (which gates the whole feature), the
/// per-backup reports behind the health column, the diagnostics dialog, and the
/// per-backup health-monitor configuration.
/// </summary>
public partial class BackupsPanel
{
    // Availability gates the whole feature (true only when the server's backup
    // sink is durable/external); the per-backup reports back the health column and
    // diagnostics dialog; the editor fields back the per-backup health-monitor
    // config in the schedule modal.
    private bool _healthAvailable;
    private readonly Dictionary<string, BackupHealthReport> _health = new(StringComparer.Ordinal);
    private BackupHealthReport? _healthDialog;
    private bool _editHealthEnabled = true;
    private int _editHealthHours = 6;
    private int _editHealthMinutes;

    /// <summary>
    /// Whether periodic backup-health monitoring is available on this server. The
    /// catalogue hides its health column entirely when it is not.
    /// </summary>
    internal bool HealthAvailable => _healthAvailable;

    /// <summary>The health report the diagnostics dialog is showing, or <see langword="null"/>.</summary>
    internal BackupHealthReport? HealthDialog => _healthDialog;

    /// <summary>Whether the per-backup health monitor is enabled in the editor.</summary>
    internal bool EditHealthEnabled => _editHealthEnabled;

    /// <summary>The hours component of the edited health-verification interval.</summary>
    internal int EditHealthHours => _editHealthHours;

    /// <summary>The minutes component of the edited health-verification interval.</summary>
    internal int EditHealthMinutes => _editHealthMinutes;

    /// <summary>The edited health-verification interval.</summary>
    internal TimeSpan EditHealthInterval => new(hours: _editHealthHours, minutes: _editHealthMinutes, seconds: 0);

    /// <summary>Turns the per-backup health monitor on or off in the editor.</summary>
    /// <param name="enabled">Whether the periodic monitor verifies this backup.</param>
    internal void SetEditHealthEnabled(bool enabled) => _editHealthEnabled = enabled;

    /// <summary>Sets the hours component of the edited health-verification interval.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 168.</param>
    internal void SetEditHealthHours(object? value) => _editHealthHours = ParseInterval(value, 0, 168);

    /// <summary>Sets the minutes component of the edited health-verification interval.</summary>
    /// <param name="value">The raw input value, clamped to 0 - 59.</param>
    internal void SetEditHealthMinutes(object? value) => _editHealthMinutes = ParseInterval(value, 0, 59);

    /// <summary>
    /// The health of a display row: the worst-status report across its members,
    /// so a backup set flags a warning when any one member is unresolvable.
    /// <see langword="null"/> when no member has a stored report yet.
    /// </summary>
    /// <param name="row">The display row to report on.</param>
    internal BackupHealthReport? RowHealth(BackupRow row)
    {
        BackupHealthReport? worst = null;
        var members = row.Members;
        for (var i = 0; i < members.Count; i++)
        {
            if (_health.TryGetValue(members[i].Id, out var report)
                && (worst is null || report.Status > worst.Status))
            {
                worst = report;
            }
        }

        return worst;
    }

    /// <summary>Opens the health diagnostics dialog on a report.</summary>
    /// <param name="report">The report to explain.</param>
    internal void OpenHealthDialog(BackupHealthReport report)
    {
        _healthDialog = report;
        NotifyStateChanged();
    }

    /// <summary>Closes the health diagnostics dialog.</summary>
    internal void CloseHealthDialog()
    {
        _healthDialog = null;
        NotifyStateChanged();
    }

    /// <summary>
    /// Runs a fresh on-demand verification, refreshes the page's stored reports,
    /// and re-opens the dialog on the updated report so the operator sees the
    /// new result.
    /// </summary>
    /// <param name="backupId">The backup to verify.</param>
    internal async Task RecheckHealthAsync(string backupId)
    {
        BeginBusy();
        try
        {
            _lastResult = await Reader.CheckHealthAsync(backupId);
            await RefreshHealthAsync();
            _healthDialog = _health.TryGetValue(backupId, out var report) ? report : null;
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Persists the per-backup health-monitor override from the schedule modal,
    /// then refreshes the page's reports so a newly enabled backup starts
    /// showing state.
    /// </summary>
    internal async Task SaveHealthConfigAsync()
    {
        var row = _pendingSchedule;
        if (row is null)
        {
            return;
        }

        var interval = _editHealthEnabled ? EditHealthInterval : TimeSpan.FromHours(6);
        if (_editHealthEnabled && interval <= TimeSpan.Zero)
        {
            return;
        }

        BeginBusy();
        try
        {
            _lastResult = await Reader.ConfigureHealthAsync(row.Members[0].Id, _editHealthEnabled, interval);
            await RefreshHealthAsync();
        }
        finally
        {
            EndBusy();
        }
    }

    // Loads the latest stored health report for every backup visible on the
    // current page (each set member individually), so the health column and the
    // per-row indicator reflect the freshest verification the monitor persisted.
    // Inert (and cheap) when health monitoring is unavailable.
    private async Task RefreshHealthAsync()
    {
        if (!_healthAvailable)
        {
            return;
        }

        _health.Clear();
        var rows = Rows;
        for (var i = 0; i < rows.Count; i++)
        {
            var members = rows[i].Row.Members;
            for (var m = 0; m < members.Count; m++)
            {
                var id = members[m].Id;
                var report = await Reader.GetHealthAsync(id);
                if (report is not null)
                {
                    _health[id] = report;
                }
            }
        }
    }
}
