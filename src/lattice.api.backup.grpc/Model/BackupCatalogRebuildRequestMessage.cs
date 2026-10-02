namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the <c>StartCatalogRebuild</c> accept-then-poll RPC: the
/// optional idempotency id of the tracked catalog rebuild it creates.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCatalogRebuildRequestMessage)]
[Immutable]
public sealed record BackupCatalogRebuildRequestMessage
{
    /// <summary>The idempotency id of the tracked operation, or <see langword="null"/> to generate one.</summary>
    [Id(0)] public string? TrackingOperationId { get; init; }
}
