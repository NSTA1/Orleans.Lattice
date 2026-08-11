namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the explicit tree-creation lifecycle RPC: a tree id plus the
/// optional initial structural sizing (physical shard count, leaf fan-out, internal
/// fan-out). Each sizing field is nullable so the caller can defer to the library
/// defaults; supplied sizing is honoured only when the tree is created for the first
/// time (creation is idempotent).
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminCreateRequest)]
[Immutable]
public sealed record TreeAdminCreateRequest
{
    /// <summary>The tree id to create.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The initial physical shard count, or <see langword="null"/> for the library default.</summary>
    [Id(1)] public int? ShardCount { get; init; }

    /// <summary>The initial maximum keys per leaf node, or <see langword="null"/> for the library default.</summary>
    [Id(2)] public int? MaxLeafKeys { get; init; }

    /// <summary>The initial maximum children per internal node, or <see langword="null"/> for the library default.</summary>
    [Id(3)] public int? MaxInternalChildren { get; init; }
}
