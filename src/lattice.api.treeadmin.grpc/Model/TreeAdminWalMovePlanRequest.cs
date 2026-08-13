namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the WAL move plan RPC: a tree id plus the WAL partition index
/// and the target storage provider key to preview a move to. The parameters are
/// carried on the wire so the facade applies the same argument validation and
/// <c>InvalidArgument</c> / <c>OutOfRange</c> mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminWalMovePlanRequest)]
[Immutable]
public sealed record TreeAdminWalMovePlanRequest
{
    /// <summary>The tree whose partition would be moved.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The WAL partition index to preview.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>The target storage provider key.</summary>
    [Id(2)] public required string TargetProviderKey { get; init; }
}
