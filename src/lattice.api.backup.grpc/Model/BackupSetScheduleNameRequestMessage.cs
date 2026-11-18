namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the backup-set-schedule RPCs that name one schedule: the
/// cancel (pause), status and run-now RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleNameRequestMessage)]
[Immutable]
public sealed record BackupSetScheduleNameRequestMessage
{
    /// <summary>The schedule's tenant-local name.</summary>
    [Id(0)] public required string Name { get; init; }
}
