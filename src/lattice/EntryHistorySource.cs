namespace Orleans.Lattice;

/// <summary>
/// Identifies which substrate produced an <see cref="EntryHistoryPage"/>, so a
/// caller can tell a clean, age-bounded timeline (the durable history view) apart
/// from a best-effort, garbage-collection-bounded suffix (the retained source
/// write-ahead-log window).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.EntryHistorySource)]
public enum EntryHistorySource
{
    /// <summary>
    /// No history is available: the tree has no durable history view enabled and
    /// no retained write-ahead-log window could be read (the replication read
    /// seam is not registered). The page is empty.
    /// </summary>
    None = 0,

    /// <summary>
    /// The revisions were read from the durable per-key history view via a prefix
    /// scan over its view tree. This path is bounded only by the view's configured
    /// retention age, not by source write-ahead-log garbage collection, so it
    /// never reports truncation - <see cref="EntryHistoryPage.Truncated"/> is
    /// always <see langword="false"/>.
    /// </summary>
    View = 1,

    /// <summary>
    /// The revisions were read from the retained source write-ahead-log window as
    /// a best-effort fallback for a tree with no history view. The window is
    /// bounded below by the per-partition write-ahead-log garbage-collection trim
    /// point, so it can report only the surviving suffix and flags truncation via
    /// <see cref="EntryHistoryPage.Truncated"/> and
    /// <see cref="EntryHistoryPage.EarliestAvailable"/>.
    /// </summary>
    WalWindow = 2,
}
