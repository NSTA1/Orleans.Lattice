namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the accept-then-poll <c>StartBackupHealthCheck</c> RPC: the id
/// of the backup to verify against the durable sink and whose fresh report to
/// persist.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthCheckRequestMessage)]
[Immutable]
public sealed record BackupHealthCheckRequestMessage
{
    /// <summary>The backup id to verify.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>
    /// The idempotency id of the tracked operation the <c>StartBackupHealthCheck</c>
    /// RPC creates, or <see langword="null"/> to generate one.
    /// </summary>
    [Id(1)] public string? TrackingOperationId { get; init; }
}
