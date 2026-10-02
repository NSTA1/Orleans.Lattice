namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the <c>StartCatalogScrub</c> accept-then-poll RPC: whether to
/// prune the orphan rows found, and the optional idempotency id of the tracked
/// catalog scrub it creates.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCatalogScrubRequestMessage)]
[Immutable]
public sealed record BackupCatalogScrubRequestMessage
{
    /// <summary><see langword="true"/> to remove orphan rows; <see langword="false"/> to flag them only.</summary>
    [Id(0)] public bool PruneOrphans { get; init; }

    /// <summary>The idempotency id of the tracked operation, or <see langword="null"/> to generate one.</summary>
    [Id(1)] public string? TrackingOperationId { get; init; }
}
