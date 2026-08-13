namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the WAL move reclaim RPC: a tree id, the WAL partition index,
/// and the orphaned source storage provider key to reclaim. The parameters are
/// carried on the wire so the facade applies the same argument validation, tree
/// gating, and <c>InvalidArgument</c> / <c>OutOfRange</c> / <c>FailedPrecondition</c>
/// mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminWalReclaimRequest)]
[Immutable]
public sealed record TreeAdminWalReclaimRequest
{
    /// <summary>The tree whose moved source to reclaim.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The WAL partition index whose orphaned source to reclaim.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>The provider key of the orphaned source tail.</summary>
    [Id(2)] public required string SourceProviderKey { get; init; }
}
