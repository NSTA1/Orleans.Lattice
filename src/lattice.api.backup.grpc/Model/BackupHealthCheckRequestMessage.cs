namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the check-backup-health RPC and its accept-then-poll
/// <c>StartBackupHealthCheck</c> twin: the id of the backup to verify against the
/// durable sink and whose fresh report to persist.
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
    /// RPC creates, or <see langword="null"/> to generate one. Ignored by the
    /// deprecated blocking RPC.
    /// </summary>
    [Id(1)] public string? TrackingOperationId { get; init; }
}
