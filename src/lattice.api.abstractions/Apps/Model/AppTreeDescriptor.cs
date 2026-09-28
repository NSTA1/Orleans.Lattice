namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// A manifest tree declaration. Name is app-local, never a composed physical id;
/// an adoption id is explicitly declared manifest content and may be inspected.
/// </summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppTreeDescriptor), Immutable]
public sealed record AppTreeDescriptor
{
    /// <summary>The app-local tree name, never a namespace-composed identifier.</summary>
    [Id(0)] public required string Name { get; init; }
    /// <summary>Whether the app declares the tree rebuildable from other data.</summary>
    [Id(1)] public bool Rebuildable { get; init; }
    /// <summary>The operator-declared legacy tree to adopt; null creates an app-local tree.</summary>
    [Id(2)] public string? AdoptedTreeId { get; init; }
    /// <summary>The requested initial shard count, or null to inherit host defaults.</summary>
    [Id(3)] public int? ShardCount { get; init; }
    /// <summary>The creation-time virtual shard count; upgrades cannot change it.</summary>
    [Id(4)] public int? VirtualShardCount { get; init; }
    /// <summary>The requested maximum keys per leaf, or null for host defaults.</summary>
    [Id(5)] public int? MaxLeafKeys { get; init; }
    /// <summary>The requested maximum internal-node children, or null for host defaults.</summary>
    [Id(6)] public int? MaxInternalChildren { get; init; }
    /// <summary>The requested WAL partition count, or null for host defaults.</summary>
    [Id(7)] public int? WalPartitions { get; init; }
    /// <summary>The requested soft-delete retention, or null for host defaults.</summary>
    [Id(8)] public TimeSpan? SoftDeleteDuration { get; init; }
    /// <summary>
    /// Why an install of this app in the caller's tenant could not own the tree (another app owns
    /// it, it is a pre-existing unowned tree, a derived copy, or another tree's alias target), or
    /// null when it could. Reported before install so the conflict is visible when reviewing consent;
    /// an install or upgrade with any conflict is refused.
    /// </summary>
    [Id(9)] public string? OwnershipConflict { get; init; }
}
