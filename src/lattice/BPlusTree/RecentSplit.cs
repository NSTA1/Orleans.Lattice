namespace Orleans.Lattice;

/// <summary>
/// A record of a recently committed adaptive shard split, surfaced by
/// <see cref="ILattice.DiagnoseAsync"/> via <see cref="TreeDiagnosticReport.RecentSplits"/>.
/// Consolidation folds are reflected by shard-count and per-shard migration state,
/// but are not recorded in this ring. The diagnostics grain retains the most
/// recent split commits (up to 32), ordered from oldest to newest.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RecentSplit)]
[Immutable]
public readonly record struct RecentSplit
{
    /// <summary>Zero-based physical shard index that was split.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>UTC timestamp when the split committed.</summary>
    [Id(1)] public DateTime AtUtc { get; init; }
}
