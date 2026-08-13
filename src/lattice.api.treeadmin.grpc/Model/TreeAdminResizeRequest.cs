namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the online-resize trigger RPC: a tree id plus the new B+ node
/// capacity (maximum keys per leaf node and maximum children per internal node) to
/// rebuild the tree with. The target capacity is carried on the wire so the facade
/// applies the same argument validation and <c>InvalidArgument</c> / <c>OutOfRange</c>
/// mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminResizeRequest)]
[Immutable]
public sealed record TreeAdminResizeRequest
{
    /// <summary>The tree to resize.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The new maximum number of keys per leaf node.</summary>
    [Id(1)] public int NewMaxLeafKeys { get; init; }

    /// <summary>The new maximum number of children per internal node.</summary>
    [Id(2)] public int NewMaxInternalChildren { get; init; }
}
