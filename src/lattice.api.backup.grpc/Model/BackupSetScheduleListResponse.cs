using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the list-backup-set-schedules RPC: the statuses of the
/// caller's readable backup-set schedules, in name order.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleListResponse)]
[Immutable]
public sealed record BackupSetScheduleListResponse
{
    /// <summary>The schedules' statuses.</summary>
    [Id(0)] public IReadOnlyList<BackupSetScheduleStatus> Schedules { get; init; } = Array.Empty<BackupSetScheduleStatus>();
}
