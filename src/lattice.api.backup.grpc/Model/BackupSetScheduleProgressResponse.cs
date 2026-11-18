using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the backup-set-schedule run-progress RPC. A missing schedule
/// is represented by <see cref="Found"/> set to <see langword="false"/>; a schedule
/// with no run in flight by <see cref="Found"/> set to <see langword="true"/> and a
/// <see langword="null"/> <see cref="Progress"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleProgressResponse)]
[Immutable]
public sealed record BackupSetScheduleProgressResponse
{
    /// <summary>Whether a schedule with the requested name exists.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The in-flight run's progress, or <see langword="null"/> when no run is in flight.</summary>
    [Id(1)] public BackupSetScheduleRunProgress? Progress { get; init; }
}
