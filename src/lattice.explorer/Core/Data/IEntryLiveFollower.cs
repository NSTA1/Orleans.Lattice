namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Follows a single key's live change tail by subscribing to the forward state
/// change feed and yielding a lightweight <see cref="EntryChangeSignal"/> for
/// each new mutation that targets the followed key. The subscription is
/// open-ended and completes only when the supplied cancellation token is
/// cancelled (on key change or tab dispose), so callers must always pass a token
/// they cancel to tear the subscription down. The follower only signals - it
/// never refetches; the Data tab does the refetch in response to a signal.
/// </summary>
public interface IEntryLiveFollower
{
    /// <summary>
    /// Opens a change subscription on <paramref name="treeId"/> and yields an
    /// <see cref="EntryChangeSignal"/> for every notification that targets
    /// <paramref name="key"/> (an exact key match, or a range delete whose
    /// half-open swept range contains the key). The stream ends when
    /// <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    IAsyncEnumerable<EntryChangeSignal> FollowAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default);
}
