namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying only a tree id, shared by the tree-administration
/// control-API RPCs that address a single tree with no further arguments (the
/// capability probe today; the whole-tree lifecycle operations that later
/// releases add).
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminTreeRequest)]
[Immutable]
public sealed record TreeAdminTreeRequest
{
    /// <summary>The tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }
}
