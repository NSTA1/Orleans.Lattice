namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the remove-backup-set-schedule RPC: the schedule to remove
/// and whether the backups of its retained sets are deleted with it.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleRemoveRequestMessage)]
[Immutable]
public sealed record BackupSetScheduleRemoveRequestMessage
{
    /// <summary>The schedule's tenant-local name.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Whether to delete the backups of the schedule's retained sets.</summary>
    [Id(1)] public bool DeleteRetainedSets { get; init; }
}
