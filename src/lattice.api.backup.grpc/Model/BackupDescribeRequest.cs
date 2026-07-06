namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the describe-backup RPC: the id of the backup whose manifest
/// and restore chain to describe.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupDescribeRequest)]
[Immutable]
public sealed record BackupDescribeRequest
{
    /// <summary>The backup id to describe.</summary>
    [Id(0)] public required string BackupId { get; init; }
}
