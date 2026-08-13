namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the per-index tag-index administration RPCs (status, reconcile):
/// the logical tag-index name the operation targets. The name is carried on the wire
/// so the facade derives the backing membership tree id authoritatively (prefixing the
/// reserved <c>tag-</c> namespace) and applies the same argument validation, backing-tree
/// gating, and <c>InvalidArgument</c> / <c>NotFound</c> / <c>FailedPrecondition</c>
/// mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminTagIndexRequest)]
[Immutable]
public sealed record TreeAdminTagIndexRequest
{
    /// <summary>The logical tag-index name the operation targets.</summary>
    [Id(0)] public required string IndexName { get; init; }
}
