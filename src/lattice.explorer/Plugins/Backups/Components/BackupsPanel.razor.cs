using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The Backups management area: lists the visible backup catalogue and triggers
/// capture, restore, and delete operations. A capture targets one or more trees
/// selected from the cluster's tree list; a multi-tree capture is a backup set,
/// optionally captured at a single cross-tree causal fence. No client-side
/// permission pre-check gates the controls: the server is the fail-closed
/// enforcement point, so every action folds a server denial into a clean "not
/// permitted" status message rather than surfacing an unhandled error.
/// <para>
/// This root component owns the area's state and operations; each concern is a
/// partial of this class (<c>.Capture</c>, <c>.Catalogue</c>,
/// <c>.Operations</c>, <c>.Schedule</c>, <c>.Health</c>) and each region of the
/// surface is its own child component that reads this panel back through its
/// <c>Owner</c> parameter. That keeps one source of truth for the state while
/// no single file carries the whole area.
/// </para>
/// <para>
/// It reaches the host through exactly one service - the per-plugin
/// <see cref="IExplorerPluginHostContextFactory"/> - and resolves its declared
/// <see cref="IBackupsDomain"/> from the context it returns. The controlled
/// domain model and the plugin's own preference namespace are the whole of what
/// it receives from the host (epic decision D3).
/// </para>
/// </summary>
public partial class BackupsPanel : ComponentBase
{
    /// <summary>
    /// The retained active sub-tab, keyed inside this plugin's own preference
    /// namespace so it cannot collide with another plugin's key.
    /// </summary>
    private const string SubTabStateKey = "backups-subtab";

    private IExplorerPluginHostContext _context = default!;
    private IBackupsDomain _domain = default!;
    private IExplorerPluginPreferences _preferences = default!;

    private BackupOperationResult? _lastResult;
    private bool _busy;

    // The two Backups sub-tabs, with the active one retained as a durable
    // preference so the panel reopens on the same sub-tab.
    private BackupsSubTab _activeSubTab = BackupsSubTab.New;
    private bool _subTabRestored;

    /// <summary>
    /// The per-plugin host-context factory. This is the single host service the
    /// panel injects: everything else it uses is reached through the bound
    /// context, so the surface it can touch is exactly what
    /// <see cref="BackupsAreaPlugin"/> declares.
    /// </summary>
    [Inject]
    public IExplorerPluginHostContextFactory HostContexts { get; set; } = default!;

    /// <summary>Whether an operation is in flight, so the controls disable.</summary>
    internal bool Busy => _busy;

    /// <summary>The outcome of the last operation, or <see langword="null"/> before the first.</summary>
    internal BackupOperationResult? LastResult => _lastResult;

    /// <summary>The Backups plugin's controlled domain model.</summary>
    internal IBackupsDomain Domain => _domain;

    /// <summary>The backup catalogue and operations surface the domain exposes.</summary>
    internal IBackupCatalogReader Reader => _domain.Catalog;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _context = HostContexts.Create(BackupsPluginKeys.PluginId);
        _domain = _context.GetDomain<IBackupsDomain>();
        _preferences = _context.Preferences;

        _pager = new BackupCatalogPager(Reader);
        await LoadTreesAsync();
        _healthAvailable = await Reader.IsHealthMonitoringAvailableAsync();
        await ReloadAsync();

        // Restore the prior active sub-tab so the panel reopens where it was.
        // Tolerant of an unreachable backing store (server prerender); the
        // first-render fallback re-applies once browser storage is reachable.
        await _preferences.EnsureLoadedAsync();
        RestoreSubTab();
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender || _subTabRestored)
        {
            return;
        }

        await _preferences.EnsureLoadedAsync();

        // Restore the retained sub-tab if the mirror hydrated; otherwise fall
        // back to the default so a body renders rather than staying blank when
        // browser storage is unreachable (for example with JavaScript disabled).
        RestoreSubTab();
        if (!_subTabRestored)
        {
            _subTabRestored = true;
        }

        StateHasChanged();
    }

    /// <summary>
    /// Re-renders the panel and, with it, every child region.
    /// <para>
    /// A child component's own event handler only re-renders that child, so a
    /// region that mutates panel state calls this to keep the header, the
    /// result banner, and the sibling regions in step. Blazor coalesces the
    /// notification with the child's own automatic re-render into a single
    /// batch, so this costs a flag rather than an extra render pass.
    /// </para>
    /// </summary>
    internal void NotifyStateChanged() => StateHasChanged();

    /// <summary>
    /// Reloads the catalogue facets and the first page, then refreshes health.
    /// The area's Refresh command.
    /// </summary>
    internal async Task ReloadAsync()
    {
        BeginBusy();
        try
        {
            _summary = await Reader.LoadSummaryAsync();
            await _pager.ResetAsync(ExistingPageSize, CurrentFilter);
            InvalidateRows();
            _highlightedBackupIds.Clear();
            await RefreshHealthAsync();
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>Switches the active sub-tab and retains the choice.</summary>
    /// <param name="tab">The sub-tab to activate.</param>
    internal async Task SetSubTabAsync(BackupsSubTab tab)
    {
        if (_activeSubTab == tab)
        {
            return;
        }

        _activeSubTab = tab;
        await _preferences.SetAsync(SubTabStateKey, tab);
        StateHasChanged();
    }

    /// <summary>Shortens a content-addressed id for display.</summary>
    /// <param name="id">The id to shorten. Must not be <see langword="null"/>.</param>
    internal static string Shorten(string id) => id.Length <= 12 ? id : id[..12] + "...";

    /// <summary>The status modifier class for the result banner.</summary>
    /// <param name="status">The last operation's status.</param>
    internal static string ResultClass(BackupOperationStatus status) => status switch
    {
        BackupOperationStatus.Succeeded => "is-success",
        BackupOperationStatus.Denied => "is-denied",
        _ => "is-failed",
    };

    /// <summary>
    /// Clamps a numeric input to a range, falling back to the minimum for
    /// anything unparseable, so an interval editor never carries a bad value.
    /// </summary>
    /// <param name="value">The raw input value.</param>
    /// <param name="min">The inclusive lower bound, and the fallback.</param>
    /// <param name="max">The inclusive upper bound.</param>
    internal static int ParseInterval(object? value, int min, int max)
    {
        if (int.TryParse(value?.ToString(), out var parsed))
        {
            return Math.Clamp(parsed, min, max);
        }

        return min;
    }

    private void RestoreSubTab()
    {
        if (_subTabRestored || !_preferences.IsLoaded)
        {
            return;
        }

        _subTabRestored = true;
        _activeSubTab = _preferences.GetOrDefault(SubTabStateKey, BackupsSubTab.New);
    }

    // A busy transition is rendered rather than merely recorded: the panel's own
    // header and every child region read it, and a child's automatic re-render
    // does not reach the panel.
    private void BeginBusy()
    {
        _busy = true;
        StateHasChanged();
    }

    private void EndBusy()
    {
        _busy = false;
        StateHasChanged();
    }
}
