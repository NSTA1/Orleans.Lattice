using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the schedule-backup-set and backup-set-schedule-status RPCs.
/// A missing schedule is represented by <see cref="Found"/> set to
/// <see langword="false"/> and a <see langword="null"/> <see cref="Status"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleStatusResponse)]
[Immutable]
public sealed record BackupSetScheduleStatusResponse
{
    /// <summary>Whether a schedule with the requested name exists.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The schedule's status, or <see langword="null"/> when <see cref="Found"/> is <see langword="false"/>.</summary>
    [Id(1)] public BackupSetScheduleStatus? Status { get; init; }
}
