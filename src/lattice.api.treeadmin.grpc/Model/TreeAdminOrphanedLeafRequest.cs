namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying a tree id and an optional resume position, used by the
/// two orphaned-leaf tree-administration control-API RPCs.
/// </summary>
/// <remarks>
/// Both verbs return one bounded batch per call, so a whole pass is a sequence of
/// calls in which each carries back the previous reply's resume position. Carrying
/// it in a request record of its own, rather than reusing the tree-only request,
/// keeps the pass drivable across the wire.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminOrphanedLeafRequest)]
[Immutable]
public sealed record TreeAdminOrphanedLeafRequest
{
    /// <summary>The tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The previous batch's resume position, passed back unaltered, or
    /// <see langword="null"/> to start a new pass at the first shard.
    /// </summary>
    [Id(1)] public string? ResumeFrom { get; init; }
}
