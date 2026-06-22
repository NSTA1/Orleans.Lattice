namespace Orleans.Lattice.Api.State;

/// <summary>
/// A single entry in the tree catalog returned by
/// <see cref="ILatticeStateQuery.ListTreesAsync"/>: the lightweight
/// "what trees exist?" projection the explorer opens with, sourced from the
/// internal tree registry without the caller talking to registry grains.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeCatalogEntry)]
[Immutable]
public sealed record TreeCatalogEntry
{
    /// <summary>The logical tree id as registered in the cluster.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when this logical id is an alias that resolves to
    /// a different physical tree (set after a resize). <see cref="PhysicalTreeId"/>
    /// carries the resolved id in that case.
    /// </summary>
    [Id(1)] public bool IsAlias { get; init; }

    /// <summary>
    /// The physical tree id this logical id resolves to when <see cref="IsAlias"/>
    /// is <see langword="true"/>; <see langword="null"/> when the logical id is
    /// its own physical id (the common case).
    /// </summary>
    [Id(2)] public string? PhysicalTreeId { get; init; }

    /// <summary>The tree's lifecycle state (active / soft-deleted / purging).</summary>
    [Id(3)] public TreeLifecycleState Lifecycle { get; init; }

    /// <summary>The number of physical shards configured for the tree.</summary>
    [Id(4)] public int ShardCount { get; init; }

    /// <summary>The tree's effective, read-only configuration.</summary>
    [Id(5)] public required TreeConfigSummary Config { get; init; }
}
