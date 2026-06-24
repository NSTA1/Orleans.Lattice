namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Forward-only-safe pager over the Data tab's snapshot scan. The state-API
/// snapshot cursor is forward-only: every page's continuation token names the
/// <em>same</em> server cursor, which advances by one page on each call and is
/// closed once drained. Replaying a token for a page that has already been read
/// therefore either skips ahead silently or, once the cursor is closed, makes
/// the server reject the call with <c>InvalidArgument</c>.
/// <para>
/// To support backward navigation without ever replaying a token, this pager
/// caches the contents of every page it has visited and only calls
/// <see cref="IDataReader.ScanAsync"/> when advancing the frontier (visiting a
/// page for the first time). Revisiting an already-seen page is served from the
/// cache.
/// </para>
/// </summary>
public sealed class DataPager(IDataReader reader)
{
    private readonly IDataReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));
    private readonly List<DataPage> _pages = new();

    // The continuation token of the current snapshot's still-open (undrained)
    // server cursor, or null when the scan has been fully drained (the server
    // closes the cursor on the final page). Tracked so an abandoned scan -
    // superseded by a reset or torn down on dispose - can release its cursor
    // promptly instead of leaking it until the idle TTL.
    private string? _liveContinuation;

    /// <summary>The tree or view id the current snapshot scans, or <see langword="null"/> before the first reset.</summary>
    public string? TreeId { get; private set; }

    /// <summary>The page size the current snapshot was opened with.</summary>
    public int PageSize { get; private set; } = DataPaging.DefaultPageSize;

    /// <summary>The tag filter the current snapshot was opened with, if any.</summary>
    public TagFilter? TagFilter { get; private set; }

    /// <summary>The zero-based index of the page currently in view.</summary>
    public int PageIndex { get; private set; }

    /// <summary>The page currently in view, or <see cref="DataPage.Empty"/> before the first reset.</summary>
    public DataPage Current => _pages.Count == 0 ? DataPage.Empty : _pages[PageIndex];

    /// <summary>Whether a previous (already-visited) page exists.</summary>
    public bool CanGoPrevious => PageIndex > 0;

    /// <summary>
    /// Whether a next page exists, either an already-cached page ahead of the
    /// current one or a further page available at the frontier.
    /// </summary>
    public bool CanGoNext => _pages.Count > 0 && (PageIndex < _pages.Count - 1 || Current.HasMore);

    /// <summary>
    /// Opens a fresh point-in-time snapshot from the first page, discarding any
    /// previously cached pages. On failure the previously cached pages are left
    /// untouched so the caller can surface the error and retry. When the prior
    /// snapshot still had an open cursor, it is released best-effort after the
    /// new snapshot opens so its server-side WAL pin and baseline are freed
    /// promptly rather than lingering until the idle TTL.
    /// </summary>
    public async Task ResetAsync(
        string treeId,
        int pageSize,
        TagFilter? tagFilter = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Capture the outgoing snapshot's live cursor before mutating state so a
        // scan failure leaves both the cached pages and the old cursor intact.
        var oldTreeId = TreeId;
        var oldContinuation = _liveContinuation;

        var page = await _reader.ScanAsync(treeId, pageSize, null, tagFilter, cancellationToken).ConfigureAwait(false);

        TreeId = treeId;
        PageSize = pageSize;
        TagFilter = tagFilter;
        _pages.Clear();
        _pages.Add(page);
        PageIndex = 0;
        _liveContinuation = page.HasMore ? page.ContinuationToken : null;

        await CancelCursorAsync(oldTreeId, oldContinuation, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Moves to the next page. An already-visited page is served from the cache;
    /// only the frontier advance calls the reader (using the frontier page's
    /// continuation token, the single position at which it is valid). A no-op
    /// when there is no next page.
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

        var token = _pages[^1].ContinuationToken;
        if (string.IsNullOrEmpty(token))
        {
            return;
        }

        var page = await _reader.ScanAsync(TreeId!, PageSize, token, TagFilter, cancellationToken).ConfigureAwait(false);
        _pages.Add(page);
        PageIndex = _pages.Count - 1;

        // The cursor advanced in place; the prior frontier token is now consumed,
        // so the live cursor is the new frontier (or drained, if this was last).
        _liveContinuation = page.HasMore ? page.ContinuationToken : null;
    }

    /// <summary>Moves to the previous (cached) page. A no-op on the first page.</summary>
    public void Previous()
    {
        if (PageIndex > 0)
        {
            PageIndex--;
        }
    }

    /// <summary>
    /// Releases the current snapshot's open cursor (if any) best-effort. Intended
    /// for tab teardown so an abandoned scan does not retain its server-side WAL
    /// pin and baseline until the idle TTL. Idempotent; safe to call repeatedly.
    /// </summary>
    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var treeId = TreeId;
        var continuation = _liveContinuation;
        _liveContinuation = null;
        await CancelCursorAsync(treeId, continuation, cancellationToken).ConfigureAwait(false);
    }

    private async Task CancelCursorAsync(string? treeId, string? continuationToken, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(treeId) || string.IsNullOrEmpty(continuationToken))
        {
            return;
        }

        await _reader.CancelScanAsync(treeId, continuationToken, cancellationToken).ConfigureAwait(false);
    }
}
