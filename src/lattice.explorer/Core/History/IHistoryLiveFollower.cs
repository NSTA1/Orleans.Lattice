namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Follows a single key's live change tail by subscribing to the forward state
/// change feed and yielding a lightweight live-tail row for each new, matching,
/// not-yet-seen mutation. The subscription is open-ended and completes only when
/// the supplied cancellation token is cancelled (on key change or tab dispose),
/// so callers must always pass a token they cancel to tear the subscription down.
/// </summary>
public interface IHistoryLiveFollower
{
    /// <summary>
    /// Opens a change subscription on <paramref name="treeId"/> and yields a
    /// live-tail row for every notification that <paramref name="tail"/> accepts
    /// (targets the followed key and is not a duplicate of an already-loaded
    /// revision). The stream ends when <paramref name="cancellationToken"/> is
    /// cancelled.
    /// </summary>
    IAsyncEnumerable<HistoryRevisionRow> FollowAsync(
        string treeId,
        HistoryLiveTail tail,
        CancellationToken cancellationToken = default);
}
