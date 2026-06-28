namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Reads a single key's change-history timeline for the History tab over the
/// public state-API <c>GetEntryHistoryAsync</c> surface. Paging always advances
/// the timeline oldest-to-newest (the in-page order is normalised to
/// oldest-first); the display newest-first ordering is applied later by
/// <see cref="HistoryTimeline.Build"/>.
/// </summary>
public interface IHistoryReader
{
    /// <summary>
    /// Loads one page of <paramref name="key"/>'s history on
    /// <paramref name="treeId"/>. Pass the <paramref name="continuationToken"/>
    /// from a prior page to resume the timeline, or <see langword="null"/> to
    /// start at the oldest available revision.
    /// </summary>
    Task<HistoryPage> LoadAsync(
        string treeId,
        string key,
        int limit,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);
}
