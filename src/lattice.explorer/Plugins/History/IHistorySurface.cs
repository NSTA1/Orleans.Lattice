using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The controlled domain model of the per-key revision-timeline surface: the
/// retrospective page read, the forward-only live tail, the inspected key
/// another surface published, and the connection health the live indicator
/// mirrors.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). The surface never
/// receives the state-API connection itself - only its <em>state</em>, and only
/// through <see cref="ObserveConnection"/>, whose returned subscription the view
/// disposes with its own lifetime.
/// </para>
/// </summary>
public interface IHistorySurface
{
    /// <summary>The current connection health, mirrored by the live indicator.</summary>
    LatticeConnectionState ConnectionState { get; }

    /// <summary>
    /// The key another surface published as the inspected key for
    /// <paramref name="treeId"/>, or <see langword="null"/> when none has been.
    /// This is the whole of the hand-off: the surfaces share one inspected key
    /// per tree, so activating this one opens the timeline the operator meant.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    string? InspectedKey(string treeId);

    /// <summary>
    /// Subscribes <paramref name="onChanged"/> to connection-health changes.
    /// Dispose the returned subscription to unsubscribe; the view does so with
    /// its own lifetime, so no handler outlives the surface that installed it.
    /// </summary>
    /// <param name="onChanged">The handler to invoke on a change. Must not be <see langword="null"/>.</param>
    IDisposable ObserveConnection(Action<LatticeConnectionState> onChanged);

    /// <summary>
    /// Loads one page of <paramref name="key"/>'s history on
    /// <paramref name="treeId"/>. Pass the <paramref name="continuationToken"/>
    /// from a prior page to resume, or <see langword="null"/> to start at the
    /// oldest available revision.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="key">The key whose timeline to read. Must not be <see langword="null"/>.</param>
    /// <param name="limit">The page size to request.</param>
    /// <param name="continuationToken">The prior page's cursor, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<HistoryPage> LoadAsync(
        string treeId,
        string key,
        int limit,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens the forward-only live tail on <paramref name="treeId"/>, yielding a
    /// row for every change <paramref name="tail"/> accepts. The stream ends only
    /// when <paramref name="cancellationToken"/> is cancelled, so the caller must
    /// always pass a token it cancels.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="tail">The de-duplication tail seeded with the loaded revisions.</param>
    /// <param name="cancellationToken">Cancelled to tear the subscription down.</param>
    IAsyncEnumerable<HistoryRevisionRow> FollowAsync(
        string treeId,
        HistoryLiveTail tail,
        CancellationToken cancellationToken = default);
}
