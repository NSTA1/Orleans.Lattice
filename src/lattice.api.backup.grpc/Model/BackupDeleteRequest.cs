namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the delete-backup RPC: the id of the backup to remove from
/// the catalog and sink.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupDeleteRequest)]
[Immutable]
public sealed record BackupDeleteRequest
{
    /// <summary>The backup id to delete.</summary>
    [Id(0)] public required string BackupId { get; init; }
}
