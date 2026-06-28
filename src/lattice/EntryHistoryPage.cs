using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// One page of a key's revision timeline returned by
/// <see cref="ILattice.ScanEntryHistoryAsync"/>. Revisions are ordered
/// oldest-first by <see cref="EntryRevision.Hlc"/>. Paging is continuation-based:
/// a non-<see langword="null"/> <see cref="Continuation"/> means more revisions
/// are available and should be fetched by passing it back to the next call.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.EntryHistoryPage)]
[Immutable]
public sealed record EntryHistoryPage
{
    /// <summary>The revisions in this page, ordered oldest-first by <see cref="EntryRevision.Hlc"/>.</summary>
    [Id(0)] public IReadOnlyList<EntryRevision> Revisions { get; init; } = Array.Empty<EntryRevision>();

    /// <summary>
    /// An opaque token to fetch the next page, or <see langword="null"/> when this
    /// is the last page. Pass it back as the <c>continuation</c> argument of the
    /// next <see cref="ILattice.ScanEntryHistoryAsync"/> call.
    /// </summary>
    [Id(1)] public string? Continuation { get; init; }

    /// <summary>
    /// Whether revisions older than <see cref="EarliestAvailable"/> may have
    /// existed but are no longer readable. Always <see langword="false"/> on the
    /// <see cref="EntryHistorySource.View"/> path (the history view retains a clean
    /// timeline bounded only by age); can be <see langword="true"/> on the
    /// <see cref="EntryHistorySource.WalWindow"/> fallback once write-ahead-log
    /// garbage collection has trimmed the oldest entries, so a partial window is
    /// never mistaken for full history.
    /// </summary>
    [Id(2)] public bool Truncated { get; init; }

    /// <summary>
    /// On a truncated <see cref="EntryHistorySource.WalWindow"/> page, the
    /// hybrid-logical-clock timestamp of the oldest still-readable revision: history
    /// before this point was trimmed by write-ahead-log garbage collection.
    /// <see cref="HybridLogicalClock.Zero"/> when nothing was trimmed or no
    /// revisions were found.
    /// </summary>
    [Id(3)] public HybridLogicalClock EarliestAvailable { get; init; }

    /// <summary>Which substrate produced this page.</summary>
    [Id(4)] public EntryHistorySource Source { get; init; }
}
