using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the schedule-backup-set RPC. Carries the
/// <see cref="LatticeBackupSetScheduleRequest"/> directly, as an Orleans-serializable
/// record. Deserialization does not run its constructor, so the facade rebuilds the
/// request through the constructor - re-validating the name, the membership, the
/// interval and the retention bounds - before acting on it.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleRequestMessage)]
[Immutable]
public sealed record BackupSetScheduleRequestMessage
{
    /// <summary>The backup-set schedule to register or update.</summary>
    [Id(0)] public required LatticeBackupSetScheduleRequest Request { get; init; }
}
