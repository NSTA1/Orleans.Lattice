using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Default <see cref="IEntryLiveFollower"/> over the state-API change feed
/// (<c>ObserveChangesAsync</c>). It mirrors the History tab's live-tail
/// follower: scope the subscription to the tree, surface only user-driven writes
/// (<c>IncludeMaintenance = false</c>), and apply the single-key match
/// client-side. The follower never re-fetches per notification - a signal
/// carries only the notification metadata, keeping the forward feed cheap; the
/// Data tab refetches the entry in response to a signal.
/// </summary>
public sealed class EntryLiveFollower(ILatticeStateClient client) : IEntryLiveFollower
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async IAsyncEnumerable<EntryChangeSignal> FollowAsync(
        string treeId,
        string key,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);

        // One-tree-per-subscription: scope to the tree and filter to the key
        // client-side. Start from the live tail (no continuation) and exclude
        // library-maintenance churn - the Data detail follows user writes.
        var request = new StateObserveRequest
        {
            TreeId = treeId,
            IncludeMaintenance = false,
        };

        await foreach (var notification in _client.ObserveChangesAsync(request, cancellationToken).ConfigureAwait(false))
        {
            if (Covers(notification, key))
            {
                yield return new EntryChangeSignal(key, notification.Kind, notification.Hlc);
            }
        }
    }

    /// <summary>
    /// Whether a notification applies to <paramref name="key"/>: an exact key
    /// match, or a range delete whose half-open swept range
    /// <c>[Key, EndExclusiveKey)</c> contains the key. Mirrors the History tab's
    /// coverage test so the two live paths agree on what a range delete touches.
    /// </summary>
    private static bool Covers(StateChangeNotification notification, string key)
    {
        if (string.Equals(notification.Key, key, StringComparison.Ordinal))
        {
            return true;
        }

        if (notification.Kind == StateChangeKind.DeleteRange && notification.EndExclusiveKey is { } end)
        {
            return string.CompareOrdinal(notification.Key, key) <= 0
                && string.CompareOrdinal(key, end) < 0;
        }

        return false;
    }
}
