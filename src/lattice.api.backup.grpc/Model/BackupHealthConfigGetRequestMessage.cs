namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the get-backup-health-config RPC: the id of the backup whose
/// effective health-monitor configuration to read.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthConfigGetRequestMessage)]
[Immutable]
public sealed record BackupHealthConfigGetRequestMessage
{
    /// <summary>The backup id whose monitor configuration to read.</summary>
    [Id(0)] public required string BackupId { get; init; }
}
