namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the WAL move execute RPC: a tree id, the WAL partition index,
/// the target storage provider key, and optional move tunables. The parameters are
/// carried on the wire so the facade applies the same argument validation, tree
/// gating, and <c>InvalidArgument</c> / <c>OutOfRange</c> mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminWalMoveExecuteRequest)]
[Immutable]
public sealed record TreeAdminWalMoveExecuteRequest
{
    /// <summary>The tree whose partition to move.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The WAL partition index to move.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>The target storage provider key.</summary>
    [Id(2)] public required string TargetProviderKey { get; init; }

    /// <summary>Optional move tunables; <c>null</c> takes the conventional defaults.</summary>
    [Id(3)] public TreeWalMoveOptions? Options { get; init; }
}
