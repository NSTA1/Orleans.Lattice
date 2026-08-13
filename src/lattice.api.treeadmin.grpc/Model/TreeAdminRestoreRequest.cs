namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the <c>RestoreTree</c> RPC: the tree to restore into, the
/// content-addressed backup id to restore, and an optional idempotency key. The
/// restore always runs as a shadow-cutover, so the mode is implicit and not carried
/// on the wire.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminRestoreRequest)]
[Immutable]
public sealed record TreeAdminRestoreRequest
{
    /// <summary>The tree to restore the backup into.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The content-addressed id of the backup to restore.</summary>
    [Id(1)] public required string BackupId { get; init; }

    /// <summary>The idempotency key for the restore, or <see langword="null"/> to derive one from the request.</summary>
    [Id(2)] public string? OperationId { get; init; }
}
