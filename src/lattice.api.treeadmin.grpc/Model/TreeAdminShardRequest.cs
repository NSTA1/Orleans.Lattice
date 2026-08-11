namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying a tree id and a physical shard index, used by the
/// tree-administration control-API RPC that addresses a single shard of a tree (the
/// leaf-projection digest read).
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminShardRequest)]
[Immutable]
public sealed record TreeAdminShardRequest
{
    /// <summary>The tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The zero-based physical shard index the call targets.</summary>
    [Id(1)] public int ShardIndex { get; init; }
}
