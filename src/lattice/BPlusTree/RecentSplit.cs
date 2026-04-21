namespace Orleans.Lattice;

/// <summary>
/// A record of a recently-committed adaptive shard split, surfaced by
/// <see cref="ILattice.DiagnoseAsync"/> via <see cref="TreeDiagnosticReport.RecentSplits"/>.
/// The diagnostics grain retains a bounded ring buffer of the most recent events
/// (up to 32). Entries are ordered from oldest to newest.
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
