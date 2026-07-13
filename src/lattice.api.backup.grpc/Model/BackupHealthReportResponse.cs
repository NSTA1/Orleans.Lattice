using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response carrying a backup's health report. A missing report (no
/// verification has run for the backup) is represented by <see cref="Found"/> set
/// to <see langword="false"/> and a <see langword="null"/> <see cref="Report"/>;
/// otherwise <see cref="Report"/> is the point-in-time
/// <see cref="BackupHealthReport"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthReportResponse)]
[Immutable]
public sealed record BackupHealthReportResponse
{
    /// <summary>Whether a health report was found for the backup.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The backup's health report, or <see langword="null"/> when none exists.</summary>
    [Id(1)] public BackupHealthReport? Report { get; init; }
}
