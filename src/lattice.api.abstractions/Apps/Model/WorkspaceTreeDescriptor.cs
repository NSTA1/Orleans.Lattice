namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// An app's tree as any role holder may see it: its app-local name, shape and retention,
/// and whether it was adopted, but never a physical or adopted tree id.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.WorkspaceTreeDescriptor), Immutable]
public sealed record WorkspaceTreeDescriptor
{
    /// <summary>The app-local tree name.</summary>
    [Id(0)] public required string Name { get; init; }
    /// <summary>Whether the app declares the tree rebuildable from other data.</summary>
    [Id(1)] public bool Rebuildable { get; init; }
    /// <summary>Whether the tree adopts an operator-declared pre-existing tree.</summary>
    [Id(2)] public bool Adopted { get; init; }
    /// <summary>The declared initial shard count, or null for host defaults.</summary>
    [Id(3)] public int? ShardCount { get; init; }
    /// <summary>The declared virtual shard count, or null for host defaults.</summary>
    [Id(4)] public int? VirtualShardCount { get; init; }
    /// <summary>The declared maximum keys per leaf, or null for host defaults.</summary>
    [Id(5)] public int? MaxLeafKeys { get; init; }
    /// <summary>The declared maximum internal-node children, or null for host defaults.</summary>
    [Id(6)] public int? MaxInternalChildren { get; init; }
    /// <summary>The declared WAL partition count, or null for host defaults.</summary>
    [Id(7)] public int? WalPartitions { get; init; }
    /// <summary>The declared soft-delete retention, or null for host defaults.</summary>
    [Id(8)] public TimeSpan? SoftDeleteDuration { get; init; }
}
