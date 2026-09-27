namespace Orleans.Lattice.Apps;

/// <summary>Tree ownership and optional physical sizing pins; omitted pins inherit host defaults.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppTreeDeclaration), Immutable]
public sealed record AppTreeDeclaration
{
    /// <summary>App-local tree path segment.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Initial physical shard count, when pinned.</summary>
    [Id(1)] public int? ShardCount { get; init; }

    /// <summary>Virtual slot count fixed at creation; a manifest upgrade must not change this pin.</summary>
    [Id(2)] public int? VirtualShardCount { get; init; }

    /// <summary>Maximum leaf keys, when pinned; at least two.</summary>
    [Id(3)] public int? MaxLeafKeys { get; init; }

    /// <summary>Maximum internal children, when pinned; at least three.</summary>
    [Id(4)] public int? MaxInternalChildren { get; init; }

    /// <summary>WAL partition count, when pinned; at least one.</summary>
    [Id(5)] public int? WalPartitions { get; init; }

    /// <summary>Positive soft-delete retention, or null to inherit the host duration.</summary>
    [Id(6)] public TimeSpan? SoftDeleteDuration { get; init; }

    /// <summary>Whether app-scoped recovery may rederive this tree instead of restoring its bytes.</summary>
    [Id(7)] public bool Rebuildable { get; init; }

    /// <summary>
    /// Existing physical tree adopted instead of <c>a/{app}/{name}</c>, intended for first-party
    /// migration of pre-app trees. Adoption is never structurally granted: the install ceiling
    /// must contain operator-approved exception scopes for this tree or activation must fail.
    /// Scope templates continue to reference <see cref="Name"/>, not this physical id.
    /// </summary>
    [Id(8)] public string? AdoptedTreeId { get; init; }
}
