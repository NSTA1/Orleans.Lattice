namespace Orleans.Lattice.Api.State;

/// <summary>
/// Tells a consumer how a key's returned change history is bounded below, so it
/// can distinguish a clean, durable timeline from a best-effort fallback that
/// may have lost its oldest revisions. Mapped server-side from the backing
/// <see cref="EntryHistorySource"/> and the page's truncation flag.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryHistoryBound)]
public enum EntryHistoryBound
{
    /// <summary>
    /// The revisions came from the durable per-key history view. The timeline is
    /// bounded only by the view's configured retention age (not by source
    /// write-ahead-log garbage collection) and is never truncated: every
    /// revision still within the retention window is readable.
    /// </summary>
    BoundedByAge = 0,

    /// <summary>
    /// The revisions came from the retained source write-ahead-log window as a
    /// best-effort fallback (no history view is enabled) and that window has
    /// already been trimmed by garbage collection, so older revisions are no
    /// longer readable. <see cref="EntryHistoryResult.EarliestAvailable"/>
    /// reports the oldest still-readable revision.
    /// </summary>
    Truncated = 1,

    /// <summary>
    /// The revisions came from the retained source write-ahead-log window with no
    /// history view enabled, and no trimming has yet occurred (or no replication
    /// read seam is registered, in which case the page is empty). The history is
    /// only as deep as the live write-ahead-log window, not a durable timeline.
    /// </summary>
    WalWindowFallback = 2,
}
