namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the bulk-load session-boundary RPCs (<c>BeginBulkLoad</c> and
/// <c>CommitBulkLoad</c>): a tree id plus the caller's stable, idempotent
/// operation id. The same shape serves both boundaries because each carries only
/// the session identity; the per-chunk payload travels on
/// <see cref="TreeAdminBulkLoadAppendRequest"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminBulkLoadSessionRequest)]
[Immutable]
public sealed record TreeAdminBulkLoadSessionRequest
{
    /// <summary>The tree the bulk-load session targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The caller's stable bulk-load operation id. Must be non-empty and must not
    /// contain <c>'/'</c>; it keys the idempotent per-chunk append so a resumed
    /// stream re-drives cleanly.
    /// </summary>
    [Id(1)] public required string OperationId { get; init; }
}
