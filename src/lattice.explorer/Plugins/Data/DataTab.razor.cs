using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// The value drill-down surface's core state and lifecycle: the retained-view
/// seeding, the callbacks its child components are bound to, and the connection
/// indicator. Paging lives in <c>DataTab.Paging.cs</c>, the selected entry and
/// its live tail in <c>DataTab.Entry.cs</c>, and the tag filter in
/// <c>DataTab.Tags.cs</c>; all of them are one partial class with the markup in
/// <c>DataTab.razor</c>.
/// </summary>
public partial class DataTab
{
    private static readonly IReadOnlyList<DataEntry> NoEntries = Array.Empty<DataEntry>();

    private DataPager? _pager;
    private int _pageSize = DataPaging.DefaultPageSize;
    private bool _loading;
    private string? _error;
    private ExplorerStateMessage? _failure;

    private string _keyPrefix = string.Empty;
    private EntryScanMode _scanMode = EntryScanMode.Live;

    private bool _showLiveIndicator;
    private Type? _historyViewType;
    private LatticeConnectionState _connectionState = LatticeConnectionState.Disconnected;
    private IDisposable? _connectionSubscription;

    private bool _seededFromRetained;

    // Every callback the five child components take is bound once here rather
    // than composed in the markup, so a re-render of this surface - which happens
    // on every load, every live refresh and every page step - allocates no
    // delegates.
    private EventCallback _refresh;
    private EventCallback _previousPage;
    private EventCallback _nextPage;
    private EventCallback _retryDetail;
    private EventCallback _toggleHistory;
    private EventCallback<int> _pageSizeChanged;
    private EventCallback<EntryScanMode> _scanModeChanged;
    private EventCallback<string> _keySearch;
    private EventCallback<string> _selectKey;
    private EventCallback<string> _goToSourceTree;

    /// <summary>
    /// The ambient shell context, so the two-pane body stacks at compact width by
    /// name rather than by measuring a viewport.
    /// </summary>
    [CascadingParameter]
    public LatticeAdaptiveContext? AdaptiveContext { get; set; }

    private bool IsCompact =>
        (AdaptiveContext?.Breakpoint ?? LatticeBreakpoints.Default) == LatticeBreakpoint.Compact;

    private IReadOnlyList<DataEntry> CurrentEntries => _pager?.Current.Entries ?? NoEntries;

    /// <summary>
    /// The state the key list is in, or <see langword="null"/> when it has rows
    /// to show. A failed scan is worded as a failure with the cluster's own
    /// words and a retry, never as "no entries" - which would tell an operator
    /// the table is empty when the read never landed.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass, so it only ever selects an
    /// already-built message. The failure copy quotes the cluster's own words
    /// and therefore has to be composed; it is composed once, where the failure
    /// is caught.
    /// </remarks>
    private ExplorerStateMessage? ListState
    {
        get
        {
            if (_failure is not null)
            {
                return _failure;
            }

            return _loading && CurrentEntries.Count == 0
                ? ExplorerStateCopy.Loading(ExplorerSubjects.Entries)
                : null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _refresh = EventCallback.Factory.Create(this, ReloadFirstPageAsync);
        _previousPage = EventCallback.Factory.Create(this, PreviousPage);
        _nextPage = EventCallback.Factory.Create(this, NextPageAsync);
        _retryDetail = EventCallback.Factory.Create(this, RetrySelectedAsync);
        _toggleHistory = EventCallback.Factory.Create(this, ToggleHistory);
        _pageSizeChanged = EventCallback.Factory.Create<int>(this, OnPageSizeChangedAsync);
        _scanModeChanged = EventCallback.Factory.Create<EntryScanMode>(this, OnScanModeChangedAsync);
        _keySearch = EventCallback.Factory.Create<string>(this, OnKeySearchAsync);
        _selectKey = EventCallback.Factory.Create<string>(this, SelectAsync);
        _goToSourceTree = EventCallback.Factory.Create<string>(this, GoToSourceTree);
        BindTagCallbacks();

        // A change-history view renders the guidance card only: there is no value
        // scan, no live follower and no retained view to seed, so skip all of it
        // (the markup never touches the pager for this branch).
        if (Selection.IsHistory)
        {
            return;
        }

        _pager = Surface.CreatePager();
        _showLiveIndicator = Surface.SupportsLiveFollow;

        // The revision timeline is rendered inline in the selected-row detail
        // panel behind that row's History button, exactly as it always has been.
        // It is contributed by its own package through the nested-surface
        // registry, so this one neither references nor names it; when no head
        // registered it, there is simply no button.
        _historyViewType = Surface.EntryHistoryViewType;

        // The live indicator mirrors the shell's connection state, so a status
        // change must redraw the detail head. The subscription is owned by this
        // view and released in Dispose, so it never outlives it.
        _connectionState = Surface.ConnectionState;
        _connectionSubscription = Surface.ObserveConnection(OnConnectionStateChanged);

        // Hydrate the retained view before the first read so the controls and the
        // first scan reflect the prior view. Tolerant of an unreachable backing
        // store (a server prerender, say), in which case the first-render fallback
        // re-seeds once it becomes reachable.
        await Surface.EnsureRetainedLoadedAsync(TabToken);
        await SeedFromRetainedAndScanAsync();
        _seededFromRetained = Surface.RetainedLoaded;
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender || _seededFromRetained || Selection.IsHistory)
        {
            return;
        }

        await Surface.EnsureRetainedLoadedAsync(TabToken);
        if (!Surface.RetainedLoaded)
        {
            return;
        }

        _seededFromRetained = true;
        await SeedFromRetainedAndScanAsync();
        StateHasChanged();
    }

    private void OnConnectionStateChanged(LatticeConnectionState state) =>
        InvokeAsync(() =>
        {
            _connectionState = state;
            StateHasChanged();
        });

    /// <summary>
    /// Seeds this selection's retained view, opens the first page, replays
    /// forward to the retained page, and reopens the last inspected key.
    /// </summary>
    private async Task SeedFromRetainedAndScanAsync()
    {
        var retained = Surface.GetRetainedView(Selection.Id);
        _keyPrefix = retained.KeyPrefix;
        _pageSize = retained.PageSize;
        _scanMode = retained.ScanMode;

        // Capture the retained page before the first scan resets it; a fresh
        // snapshot always opens at page 0, so we replay forward to it afterwards.
        var targetPage = Surface.GetSessionPage(Selection.Id);

        await LoadTagIndexesAsync(retained.TagIndexName);
        await LoadRetainedTagFilterAsync();
        await ReloadFirstPageAsync();

        if (targetPage > 0)
        {
            await RestoreToPageAsync(targetPage);
        }

        // Restore the previously inspected key for this selection, if any. This
        // both reopens the last-selected key when returning to the surface and
        // lands the detail on the key when arriving from the tag-index browser
        // (which publishes the inspected key before selecting the tree).
        var restoredKey = Surface.GetInspectedKey(Selection.Id);
        if (restoredKey is not null)
        {
            await SelectAsync(restoredKey);
        }
    }

    private void GoToSourceTree(string sourceTreeId) => Surface.GoToTree(sourceTreeId);

    /// <inheritdoc />
    /// <remarks>
    /// Releases the active snapshot cursor best-effort on teardown so an
    /// abandoned scan does not retain its server-side WAL pin and baseline until
    /// the idle TTL. Fire-and-forget with a fresh token: the base disposal
    /// cancels the view token, so the cancel must not ride on it.
    /// </remarks>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connectionSubscription?.Dispose();
            _connectionSubscription = null;

            StopFollowing();

            if (_pager is not null)
            {
                _ = _pager.CloseAsync(CancellationToken.None);
            }
        }

        base.Dispose(disposing);
    }
}
