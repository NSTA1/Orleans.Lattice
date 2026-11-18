using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the schedule-backup-set RPC. Carries the
/// <see cref="LatticeBackupSetScheduleRequest"/> directly: it is itself an
/// Orleans-serializable record whose constructor validates the name, the
/// membership, the interval and the retention bounds.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleRequestMessage)]
[Immutable]
public sealed record BackupSetScheduleRequestMessage
{
    /// <summary>The backup-set schedule to register or update.</summary>
    [Id(0)] public required LatticeBackupSetScheduleRequest Request { get; init; }
}
