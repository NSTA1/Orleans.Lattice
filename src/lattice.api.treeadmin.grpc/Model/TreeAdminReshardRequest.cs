namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the online-reshard trigger RPC: a tree id plus the target
/// physical shard count to grow the tree to. The target is carried on the wire so
/// the facade applies the same grow-only argument validation and
/// <c>InvalidArgument</c> / <c>OutOfRange</c> mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminReshardRequest)]
[Immutable]
public sealed record TreeAdminReshardRequest
{
    /// <summary>The tree to reshard.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The desired number of distinct physical shards to grow the tree to.</summary>
    [Id(1)] public int TargetShardCount { get; init; }
}
