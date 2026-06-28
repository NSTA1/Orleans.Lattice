using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Default <see cref="IHistoryLiveFollower"/> over the state-API change feed
/// (<c>ObserveChangesAsync</c>). It mirrors the explorer's streaming-subscription
/// pattern: scope the subscription to the tree, surface only user-driven writes,
/// and let <see cref="HistoryLiveTail"/> apply the client-side key filter and
/// de-duplication. The follower never re-fetches per notification - a live-tail
/// row carries only the notification metadata, keeping the forward feed cheap.
/// </summary>
public sealed class HistoryLiveFollower(ILatticeStateClient client) : IHistoryLiveFollower
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async IAsyncEnumerable<HistoryRevisionRow> FollowAsync(
        string treeId,
        HistoryLiveTail tail,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(tail);

        // One-tree-per-subscription: scope to the tree and filter to the key
        // client-side. Start from the live tail (no continuation) and exclude
        // library-maintenance churn - the History tab follows user writes.
        var request = new StateObserveRequest
        {
            TreeId = treeId,
            IncludeMaintenance = false,
        };

        await foreach (var notification in _client.ObserveChangesAsync(request, cancellationToken).ConfigureAwait(false))
        {
            if (tail.TryAccept(notification, out var row) && row is not null)
            {
                yield return row;
            }
        }
    }
}
