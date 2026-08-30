using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The controlled domain model of the key and value drill-down surface: the
/// scan, the per-key read, the tag-index catalogue, the live entry tail, the
/// retained view this selection reopens on, and the two hand-offs it offers.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). Two things it
/// deliberately is not: it is not the state-API connection (only the connection
/// <em>state</em> the live indicator mirrors crosses the seam), and it is not a
/// preference store. The surface states retained-view intent - "this selection's
/// page size is now 50" - and the adapter behind the contract owns the key
/// scheme and the ownership tagging that implies.
/// </para>
/// </summary>
public interface IDataSurface
{
    /// <summary>
    /// Whether a live entry follower is registered, so the surface knows whether
    /// to show the live indicator at all. A minimal head may omit it, in which
    /// case the surface still works as a request/response view.
    /// </summary>
    bool SupportsLiveFollow { get; }

    /// <summary>
    /// Whether the retained view has been hydrated from its durable backing
    /// store. False during a server prerender, when browser storage is not yet
    /// reachable.
    /// </summary>
    bool RetainedLoaded { get; }

    /// <summary>
    /// The current connection health, mirrored by the live indicator. Reports
    /// <see cref="LatticeConnectionState.Disconnected"/> when no connection is
    /// registered.
    /// </summary>
    LatticeConnectionState ConnectionState { get; }

    /// <summary>
    /// Subscribes <paramref name="onChanged"/> to connection-health changes.
    /// Dispose the returned subscription to unsubscribe; the view does so with
    /// its own lifetime. A no-op subscription when no connection is registered.
    /// </summary>
    /// <param name="onChanged">The handler to invoke on a change. Must not be <see langword="null"/>.</param>
    IDisposable ObserveConnection(Action<LatticeConnectionState> onChanged);

    /// <summary>
    /// Creates a fresh pager over the scan. The pager caches every page it has
    /// visited, so stepping back never replays a forward-only continuation token.
    /// </summary>
    DataPager CreatePager();

    /// <summary>
    /// Fetches the full record for a single key, or <see langword="null"/> when
    /// the key is absent.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="key">The key to read. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view or the key selection is torn down.</param>
    Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>Lists the tag indexes covering <paramref name="treeId"/>, for the filter row.</summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<IReadOnlyList<TagIndexRef>> ListTagIndexesAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Lists the distinct tag values <paramref name="indexName"/> carries over <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="indexName">The index whose values to list. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<IReadOnlyList<string>> ListTagValuesAsync(
        string treeId,
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens the forward-only live tail for <paramref name="key"/>, yielding a
    /// signal for each mutation that targets it. The stream ends only when
    /// <paramref name="cancellationToken"/> is cancelled. Never called when
    /// <see cref="SupportsLiveFollow"/> is <see langword="false"/>.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="key">The key to follow. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled to tear the subscription down.</param>
    IAsyncEnumerable<EntryChangeSignal> FollowEntryAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Hydrates the retained view from its durable backing store. Tolerant of an
    /// unreachable store, so it is safe to await during initialization; the view
    /// retries once on the first interactive render.
    /// </summary>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task EnsureRetainedLoadedAsync(CancellationToken cancellationToken = default);

    /// <summary>The retained view <paramref name="treeId"/> reopens on.</summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    DataRetainedView GetRetainedView(string treeId);

    /// <summary>
    /// The retained tag value for <paramref name="treeId"/> under
    /// <paramref name="indexName"/>, or <see langword="null"/> when no filter was
    /// retained.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="indexName">The index the value belongs to. Must not be <see langword="null"/>.</param>
    string? GetRetainedTagValue(string treeId, string indexName);

    /// <summary>Retains the committed key prefix; a <see langword="null"/> or blank prefix clears it.</summary>
    Task SetRetainedKeyPrefixAsync(string treeId, string? keyPrefix, CancellationToken cancellationToken = default);

    /// <summary>Retains the chosen page size.</summary>
    Task SetRetainedPageSizeAsync(string treeId, int pageSize, CancellationToken cancellationToken = default);

    /// <summary>Retains the chosen scan isolation.</summary>
    Task SetRetainedScanModeAsync(string treeId, EntryScanMode mode, CancellationToken cancellationToken = default);

    /// <summary>Retains the chosen tag index.</summary>
    Task SetRetainedTagIndexAsync(string treeId, string indexName, CancellationToken cancellationToken = default);

    /// <summary>Retains the chosen tag value under an index; a <see langword="null"/> or blank tag clears it.</summary>
    Task SetRetainedTagValueAsync(
        string treeId,
        string indexName,
        string? tag,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// The page index this selection was left on for the session, or <c>0</c>.
    /// Page position is transient, so it is session state rather than a durable
    /// preference.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    int GetSessionPage(string treeId);

    /// <summary>Records the page index for the session; page <c>0</c> clears the entry.</summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="pageIndex">The zero-based page index in view.</param>
    void SetSessionPage(string treeId, int pageIndex);

    /// <summary>
    /// The key the operator last drilled into for this selection, or
    /// <see langword="null"/>. Shared with the revision-timeline surface, which
    /// is what makes the hand-off carry no payload.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    string? GetInspectedKey(string treeId);

    /// <summary>Publishes the key the operator drilled into for this selection.</summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="key">The inspected key. Must not be <see langword="null"/>.</param>
    void SetInspectedKey(string treeId, string key);

    /// <summary>
    /// The view registered for the per-key revision timeline, or
    /// <see langword="null"/> when no head registered one. The surface renders it
    /// inline in the selected-row detail panel behind that row's History button,
    /// and offers no button at all when this is <see langword="null"/> - so the
    /// two packages stay independent and withholding one is a complete opt-out.
    /// </summary>
    Type? EntryHistoryViewType { get; }

    /// <summary>
    /// Opens <paramref name="target"/>'s dedicated tag-index browser, optionally
    /// seeding <paramref name="tag"/> so it opens with that tag preselected.
    /// </summary>
    /// <param name="target">The tag index to browse. Must not be <see langword="null"/>.</param>
    /// <param name="tag">The tag to preselect, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task ExploreTagIndexAsync(TagIndexRef target, string? tag, CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects <paramref name="treeId"/> as a tree, so a change-history view can
    /// route the operator to the source table that holds inspectable data.
    /// </summary>
    /// <param name="treeId">The source tree id to select. Must not be <see langword="null"/>.</param>
    void GoToTree(string treeId);
}
