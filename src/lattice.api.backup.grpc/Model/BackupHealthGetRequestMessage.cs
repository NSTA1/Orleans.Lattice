namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the get-backup-health RPC: the id of the backup whose latest
/// stored health report to read.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthGetRequestMessage)]
[Immutable]
public sealed record BackupHealthGetRequestMessage
{
    /// <summary>The backup id whose stored health report to read.</summary>
    [Id(0)] public required string BackupId { get; init; }
}
