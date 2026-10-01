namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request naming one tracked backup operation, for the
/// <c>GetBackupOperationStatus</c> and <c>CancelBackupOperation</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupOperationRequestMessage)]
[Immutable]
public sealed record BackupOperationRequestMessage
{
    /// <summary>The operation id.</summary>
    [Id(0)] public required string OperationId { get; init; }
}
