using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>A backup's latest health report as a state pill.</summary>
public partial class BackupHealthPill
{
    /// <summary>The latest stored report, or <see langword="null"/> when the backup has not been checked.</summary>
    [Parameter]
    public BackupHealthReport? Report { get; set; }

    /// <summary>Whether the report is still being read.</summary>
    [Parameter]
    public bool Pending { get; set; }

    private LtStateRole Role => Pending || Report is null
        ? LtStateRole.Unknown
        : Report.Status switch
        {
            BackupHealthStatus.Healthy => LtStateRole.Healthy,
            BackupHealthStatus.Warning => LtStateRole.Drift,
            BackupHealthStatus.Missing => LtStateRole.Failed,
            _ => LtStateRole.Unknown,
        };

    private string Text => Pending
        ? "Checking"
        : Report is null ? "Not checked" : BackupsFormat.Health(Report.Status);
}
