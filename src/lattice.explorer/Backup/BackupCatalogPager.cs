namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Forward-token pager over the newest-first backup-catalog listing. The catalog
/// cursor is a stateless, opaque continuation token (an encoded index key), so
/// unlike the Data tab's live cursor a page can be re-requested at will. To keep
/// backward navigation cheap and to match the Data tab's Prev / Next / page-number
/// experience, this pager caches every page it has visited and only calls the
/// reader when advancing the frontier for the first time.
/// </summary>
public sealed class BackupCatalogPager(IBackupCatalogReader reader)
{
    private readonly IBackupCatalogReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));
    private readonly List<BackupListView> _pages = new();

    /// <summary>The page size the current snapshot was opened with.</summary>
    public int PageSize { get; private set; }

    /// <summary>The filter the current snapshot was opened with.</summary>
    public BackupCatalogFilter Filter { get; private set; } = BackupCatalogFilter.None;

    /// <summary>The zero-based index of the page currently in view.</summary>
    public int PageIndex { get; private set; }

    /// <summary>The page currently in view, or <see cref="BackupListView.Empty"/> before the first reset.</summary>
    public BackupListView Current => _pages.Count == 0 ? BackupListView.Empty : _pages[PageIndex];

    /// <summary>Whether a previous (already-visited) page exists.</summary>
    public bool CanGoPrevious => PageIndex > 0;

    /// <summary>
    /// Whether a next page exists, either an already-cached page ahead of the
    /// current one or a further page available at the frontier.
    /// </summary>
    public bool CanGoNext => _pages.Count > 0
        && (PageIndex < _pages.Count - 1 || !string.IsNullOrEmpty(Current.NextPageToken));

    /// <summary>
    /// Opens a fresh listing from the first page under <paramref name="filter"/>,
    /// discarding any previously cached pages. On failure the surfaced page carries
    /// the non-success status so the caller can render it.
    /// </summary>
    public async Task ResetAsync(int pageSize, BackupCatalogFilter filter, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);

        var page = await _reader.LoadPageAsync(pageSize, null, filter, cancellationToken).ConfigureAwait(false);

        PageSize = pageSize;
        Filter = filter;
        _pages.Clear();
        _pages.Add(page);
        PageIndex = 0;
    }

    /// <summary>
    /// Moves to the next page. An already-visited page is served from the cache;
    /// only a frontier advance calls the reader. A no-op when there is no next page.
    /// </summary>
    public async Task NextAsync(CancellationToken cancellationToken = default)
    {
        if (_pages.Count == 0)
        {
            return;
        }

        if (PageIndex < _pages.Count - 1)
        {
            PageIndex++;
            return;
        }

        var token = Current.NextPageToken;
        if (string.IsNullOrEmpty(token))
        {
            return;
        }

        var page = await _reader.LoadPageAsync(PageSize, token, Filter, cancellationToken).ConfigureAwait(false);
        _pages.Add(page);
        PageIndex = _pages.Count - 1;
    }

    /// <summary>Moves to the previous (cached) page. A no-op on the first page.</summary>
    public void Previous()
    {
        if (PageIndex > 0)
        {
            PageIndex--;
        }
    }
}
