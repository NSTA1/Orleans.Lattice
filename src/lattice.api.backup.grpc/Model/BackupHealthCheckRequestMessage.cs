namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the check-backup-health RPC: the id of the backup to verify
/// against the durable sink and whose fresh report to persist and return.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthCheckRequestMessage)]
[Immutable]
public sealed record BackupHealthCheckRequestMessage
{
    /// <summary>The backup id to verify.</summary>
    [Id(0)] public required string BackupId { get; init; }
}
