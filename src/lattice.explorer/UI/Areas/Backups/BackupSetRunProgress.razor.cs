using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Draws a running backup-set schedule's <see cref="BackupSetScheduleRunProgress"/>
/// (#6067) through the shared <c>LtProgress</c> bar: determinate only when the
/// run reports how many trees the phase has, never an invented percentage.
/// </summary>
public partial class BackupSetRunProgress
{
    /// <summary>The schedule's name, which names the bar.</summary>
    [Parameter, EditorRequired]
    public string Schedule { get; set; } = string.Empty;

    /// <summary>The run's progress.</summary>
    [Parameter, EditorRequired]
    public BackupSetScheduleRunProgress Progress { get; set; } = default!;

    /// <summary>Whether the bar announces a change of phase; off where another bar for the same run already does.</summary>
    [Parameter]
    public bool AnnouncePhase { get; set; } = true;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;
}
