namespace Orleans.Lattice.Api.State;

/// <summary>
/// Effective per-tree configuration as observed by the read-only state API.
/// Fields are read-only projections of the tree's registry entry and the
/// resolved <c>LatticeOptions</c>; the API never mutates configuration.
/// </summary>
/// <remarks>
/// <see cref="MaxLeafKeys"/> and <see cref="MaxInternalChildren"/> are
/// sourced from the per-tree registry entry and are therefore only populated
/// by the discovery endpoint, which reads the registry; a bare per-tree
/// summary leaves them <see langword="null"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.TreeConfigSummary)]
[Immutable]
public sealed record TreeConfigSummary
{
    /// <summary>Number of physical shards currently owning virtual slots.</summary>
    [Id(0)] public int ShardCount { get; init; }

    /// <summary>Total virtual slot count fixed at tree creation.</summary>
    [Id(1)] public int VirtualShardCount { get; init; }

    /// <summary>Per-tree leaf-key cap, when known from the registry entry.</summary>
    [Id(2)] public int? MaxLeafKeys { get; init; }

    /// <summary>Per-tree internal-children cap, when known from the registry entry.</summary>
    [Id(3)] public int? MaxInternalChildren { get; init; }

    /// <summary>Effective WAL partition count.</summary>
    [Id(4)] public int WalPartitions { get; init; }

    /// <summary>Effective soft-delete retention duration.</summary>
    [Id(5)] public TimeSpan SoftDeleteDuration { get; init; }
}
