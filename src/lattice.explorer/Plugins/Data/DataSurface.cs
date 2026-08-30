using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared data reader, the preference and session stores, the shell's selection,
/// the surface switcher and the plugin catalog onto <see cref="IDataSurface"/>.
/// <para>
/// Two collaborators are resolved best-effort rather than injected: a minimal
/// head (a lighter integration fixture, say) may register neither the live entry
/// follower nor the state-API connection, in which case the surface still works
/// as a request/response view. That probe lives here, on the host side of the
/// seam, so no view holds a service provider.
/// </para>
/// </summary>
/// <param name="reader">The shared data reader.</param>
/// <param name="preferences">The durable UI preference store.</param>
/// <param name="session">The session-scoped UI state store.</param>
/// <param name="selection">The shell's current-selection service.</param>
/// <param name="nested">The nested-surface registry, read only for the inline revision timeline.</param>
/// <param name="services">The container, used only for the two best-effort probes described above.</param>
internal sealed class DataSurface(
    IDataReader reader,
    IUiPreferenceStore preferences,
    IUiSessionStore session,
    IExplorerSelection selection,
    ISelectionNestedSurfaceRegistry nested,
    IServiceProvider services) : IDataSurface
{
    private readonly IDataReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    private readonly IUiPreferenceStore _preferences =
        preferences ?? throw new ArgumentNullException(nameof(preferences));

    private readonly IUiSessionStore _session = session ?? throw new ArgumentNullException(nameof(session));

    private readonly IExplorerSelection _selection =
        selection ?? throw new ArgumentNullException(nameof(selection));

    private readonly ISelectionNestedSurfaceRegistry _nested =
        nested ?? throw new ArgumentNullException(nameof(nested));

    private readonly IEntryLiveFollower? _follower =
        (services ?? throw new ArgumentNullException(nameof(services))).GetService(typeof(IEntryLiveFollower))
            as IEntryLiveFollower;

    private readonly ILatticeStateConnection? _connection =
        services.GetService(typeof(ILatticeStateConnection)) as ILatticeStateConnection;

    /// <inheritdoc />
    public bool SupportsLiveFollow => _follower is not null;

    /// <inheritdoc />
    public bool RetainedLoaded => _preferences.IsLoaded;

    /// <inheritdoc />
    public LatticeConnectionState ConnectionState =>
        _connection?.Status.State ?? LatticeConnectionState.Disconnected;

    /// <inheritdoc />
    public Type? EntryHistoryViewType => _nested.Find(SelectionNestedSurfaceKeys.EntryHistory);

    /// <inheritdoc />
    public IDisposable ObserveConnection(Action<LatticeConnectionState> onChanged)
    {
        ArgumentNullException.ThrowIfNull(onChanged);
        return _connection is null
            ? NullSubscription.Instance
            : new ConnectionSubscription(_connection, onChanged);
    }

    /// <inheritdoc />
    public DataPager CreatePager() => new(_reader);

    /// <inheritdoc />
    public Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return _reader.GetEntryAsync(treeId, key, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TagIndexRef>> ListTagIndexesAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _reader.ListTagIndexesForTreeAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListTagValuesAsync(
        string treeId,
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(indexName);
        return _reader.ListTagValuesForIndexAsync(treeId, indexName, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<EntryChangeSignal> FollowEntryAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);

        return _follower is null
            ? EmptySignals(cancellationToken)
            : _follower.FollowAsync(treeId, key, cancellationToken);
    }

    /// <inheritdoc />
    public Task EnsureRetainedLoadedAsync(CancellationToken cancellationToken = default) =>
        _preferences.EnsureLoadedAsync(cancellationToken);

    /// <inheritdoc />
    public DataRetainedView GetRetainedView(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var index = _preferences.GetOrDefault(TagIndexKey(treeId), string.Empty);
        return new DataRetainedView(
            _preferences.GetOrDefault(SearchKey(treeId), string.Empty),
            DataPaging.Normalize(_preferences.GetOrDefault(PageSizeKey(treeId), DataPaging.DefaultPageSize)),
            _preferences.GetOrDefault(ScanModeKey(treeId), EntryScanMode.Live),
            index.Length == 0 ? null : index);
    }

    /// <inheritdoc />
    public string? GetRetainedTagValue(string treeId, string indexName)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(indexName);

        var tag = _preferences.GetOrDefault(TagValueKey(treeId, indexName), string.Empty);
        return tag.Length == 0 ? null : tag;
    }

    /// <inheritdoc />
    public Task SetRetainedKeyPrefixAsync(
        string treeId,
        string? keyPrefix,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        return string.IsNullOrWhiteSpace(keyPrefix)
            ? _preferences.RemoveAsync(SearchKey(treeId), cancellationToken)
            : _preferences.SetAsync(SearchKey(treeId), keyPrefix, treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetRetainedPageSizeAsync(string treeId, int pageSize, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _preferences.SetAsync(PageSizeKey(treeId), pageSize, treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetRetainedScanModeAsync(
        string treeId,
        EntryScanMode mode,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _preferences.SetAsync(ScanModeKey(treeId), mode, treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetRetainedTagIndexAsync(
        string treeId,
        string indexName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(indexName);
        return _preferences.SetAsync(TagIndexKey(treeId), indexName, treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetRetainedTagValueAsync(
        string treeId,
        string indexName,
        string? tag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(indexName);

        return string.IsNullOrEmpty(tag)
            ? _preferences.RemoveAsync(TagValueKey(treeId, indexName), cancellationToken)
            : _preferences.SetAsync(TagValueKey(treeId, indexName), tag, treeId, cancellationToken);
    }

    /// <inheritdoc />
    public int GetSessionPage(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _session.GetOrDefault(PageKey(treeId), 0);
    }

    /// <inheritdoc />
    public void SetSessionPage(string treeId, int pageIndex)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        if (pageIndex <= 0)
        {
            _session.Remove(PageKey(treeId));
        }
        else
        {
            _session.Set(PageKey(treeId), pageIndex);
        }
    }

    /// <inheritdoc />
    public string? GetInspectedKey(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var key = _session.GetOrDefault(DataSelection.SelectedKey(treeId), string.Empty);
        return string.IsNullOrEmpty(key) ? null : key;
    }

    /// <inheritdoc />
    public void SetInspectedKey(string treeId, string key)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        _session.Set(DataSelection.SelectedKey(treeId), key);
    }

    /// <inheritdoc />
    public async Task ExploreTagIndexAsync(
        TagIndexRef target,
        string? tag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);

        if (!string.IsNullOrEmpty(tag))
        {
            // Seed the tag-index browser's one-shot preselection, owned by and
            // keyed on the membership tree id so it never collides with another
            // index's seed. This is the one place the two surfaces' retained-state
            // spellings meet; TagIndexHandOffKeyContractTests pins them together.
            await _preferences
                .SetAsync(SeededTagKey(target.TreeId), tag, target.TreeId, cancellationToken)
                .ConfigureAwait(false);
        }

        _selection.Select(new CatalogItem
        {
            Id = target.TreeId,
            Kind = CatalogKind.TagIndexes,
            IndexName = target.IndexName,
        });
    }

    /// <inheritdoc />
    public void GoToTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _selection.Select(new CatalogItem { Id = treeId, Kind = CatalogKind.Trees });
    }

    /// <summary>The durable-preference key for this selection's retained key search.</summary>
    private static string SearchKey(string treeId) => $"data-search:{treeId}";

    /// <summary>The durable-preference key for this selection's retained page size.</summary>
    private static string PageSizeKey(string treeId) => $"data-pagesize:{treeId}";

    /// <summary>The durable-preference key for this selection's retained scan isolation.</summary>
    private static string ScanModeKey(string treeId) => $"data-scanmode:{treeId}";

    /// <summary>The durable-preference key for this selection's retained tag index.</summary>
    private static string TagIndexKey(string treeId) => $"data-tagindex:{treeId}";

    /// <summary>The durable-preference key for this selection's retained tag value, per index.</summary>
    private static string TagValueKey(string treeId, string indexName) => $"data-tagvalue:{treeId}:{indexName}";

    /// <summary>The session-store key for this selection's current page index.</summary>
    private static string PageKey(string treeId) => $"data-page:{treeId}";

    /// <summary>The tag-index browser's one-shot preselected-tag key, keyed on the membership tree id.</summary>
    private static string SeededTagKey(string membershipTreeId) => $"tagindex-tag:{membershipTreeId}";

    /// <summary>
    /// The stream a head without a live follower gets: it never yields and
    /// completes only on cancellation, so the view's follow loop behaves exactly
    /// as it does against a real feed that happens to be silent.
    /// </summary>
    private static async IAsyncEnumerable<EntryChangeSignal> EmptySignals(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        yield break;
    }

    /// <summary>
    /// One view's connection-health subscription, so a torn-down view never
    /// leaves a handler behind on the circuit-scoped connection.
    /// </summary>
    private sealed class ConnectionSubscription : IDisposable
    {
        private readonly ILatticeStateConnection _connection;
        private readonly Action<LatticeConnectionStatus> _handler;
        private bool _disposed;

        public ConnectionSubscription(ILatticeStateConnection connection, Action<LatticeConnectionState> onChanged)
        {
            _connection = connection;
            _handler = status => onChanged(status.State);
            _connection.StatusChanged += _handler;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _connection.StatusChanged -= _handler;
        }
    }

    /// <summary>The subscription handed back when there is no connection to observe.</summary>
    private sealed class NullSubscription : IDisposable
    {
        internal static readonly NullSubscription Instance = new();

        private NullSubscription()
        {
        }

        public void Dispose()
        {
        }
    }
}
