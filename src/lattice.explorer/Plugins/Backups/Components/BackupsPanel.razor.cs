using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
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
    /// What the surface says while the retained sub-surface is still being
    /// resolved. Built once: it never varies, and the panel re-renders on every
    /// operation.
    /// </summary>
    private static readonly ExplorerStateMessage Restoring =
        ExplorerStateCopy.Loading(ExplorerSubjects.Backups);

    private IExplorerPluginHostContext _context = default!;
    private IBackupsDomain _domain = default!;

    private BackupOperationResult? _lastResult;
    private bool _busy;

    // The two Backups sub-surfaces. The open one is remembered on the shell's
    // declared preference contract and mirrored into the address, so a link
    // opens the surface it names and a return visit opens the surface it left.
    private BackupsSubTab _activeSubTab = BackupsSubTab.New;
    private bool _subTabRestored;
    private EventCallback<string> _selectSurface;

    /// <summary>
    /// The per-plugin host-context factory. This is the single cluster-facing
    /// host service the panel injects: everything it reads from a cluster is
    /// reached through the bound context, so the surface it can touch is exactly
    /// what <see cref="BackupsAreaPlugin"/> declares.
    /// </summary>
    [Inject]
    public IExplorerPluginHostContextFactory HostContexts { get; set; } = default!;

    /// <summary>
    /// The shell's declared preference contract, which is where the open
    /// sub-surface is remembered. Registered by <c>AddExplorerBackup</c>, which
    /// also composes the session stack, so the plugin cannot be added to a
    /// container without it.
    /// </summary>
    [Inject]
    public IExplorerShellPreferences Preferences { get; set; } = default!;

    /// <summary>
    /// The declaration catalog this area's own key is registered on when the
    /// panel mounts. Registration is idempotent by reference, so however many
    /// circuits mount the area the contract carries the key exactly once.
    /// </summary>
    [Inject]
    public IExplorerPreferenceCatalog PreferenceCatalog { get; set; } = default!;

    /// <summary>
    /// The shell's route model, which is where the open sub-surface is
    /// addressed. The address is the intent: it wins over what was remembered,
    /// because a link someone sent must show what they saw.
    /// </summary>
    [Inject]
    public IExplorerShellRouter Router { get; set; } = default!;

    /// <summary>Whether an operation is in flight, so the controls disable.</summary>
    internal bool Busy => _busy;

    /// <summary>The outcome of the last operation, or <see langword="null"/> before the first.</summary>
    internal BackupOperationResult? LastResult => _lastResult;

    /// <summary>The Backups plugin's controlled domain model.</summary>
    internal IBackupsDomain Domain => _domain;

    /// <summary>The backup catalogue and operations surface the domain exposes.</summary>
    internal IBackupCatalogReader Reader => _domain.Catalog;

    /// <summary>The sub-surface currently open.</summary>
    internal BackupsSubTab ActiveSubTab => _activeSubTab;

    /// <summary>The slug of the sub-surface currently open, which is what the strip selects on.</summary>
    internal string ActiveSurfaceId => BackupsSurfaces.SlugFor(_activeSubTab);

    /// <summary>Whether the retained sub-surface has resolved, so a body may render.</summary>
    internal bool SurfaceRestored => _subTabRestored;

    /// <summary>
    /// The strip's selection callback, bound once at mount.
    /// </summary>
    /// <remarks>
    /// Bound to a field rather than written as a method group at the call site.
    /// The panel re-renders on every operation, every busy transition and every
    /// filter keystroke, and a callback composed on the render path would be
    /// rebuilt on each of them.
    /// </remarks>
    internal EventCallback<string> SelectSurface => _selectSurface;

    /// <summary>What to say while the retained sub-surface is still resolving.</summary>
    internal static ExplorerStateMessage RestoringMessage => Restoring;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // Declared before the first read, and idempotent by reference: the
        // contract rejects an unregistered key, so this is what makes the
        // area's retained surface enumerable at /reset-view and cleared by it.
        PreferenceCatalog.Register(BackupsPluginKeys.SurfacePreference);

        // Bound once here rather than per render, for the reason given on
        // SelectSurface.
        _selectSurface = EventCallback.Factory.Create<string>(this, SelectSurfaceAsync);

        _context = HostContexts.Create(BackupsPluginKeys.PluginId);
        _domain = _context.GetDomain<IBackupsDomain>();

        _pager = new BackupCatalogPager(Reader);
        await LoadTreesAsync();
        _healthAvailable = await Reader.IsHealthMonitoringAvailableAsync();
        await ReloadAsync();

        // An address naming a surface is honoured before anything is read from
        // storage: it is an explicit intent, and it resolves synchronously, so a
        // deep link renders its surface on the first pass rather than flashing
        // the remembered one first.
        if (RestoreFromAddress())
        {
            return;
        }

        // Tolerant of an unreachable backing store (server prerender); the
        // first-render fallback re-applies once browser storage is reachable.
        await Preferences.EnsureLoadedAsync();
        RestoreSubTab();
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender || _subTabRestored)
        {
            return;
        }

        await Preferences.EnsureLoadedAsync();

        // Restore the retained sub-surface if the store hydrated; otherwise fall
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

    /// <summary>
    /// Opens the sub-surface named by <paramref name="surfaceId"/>. A slug this
    /// area does not offer is ignored, so a value that was never rendered -
    /// including one edited into the address - cannot change the surface.
    /// </summary>
    /// <param name="surfaceId">The surface slug to open.</param>
    internal Task SelectSurfaceAsync(string? surfaceId) =>
        BackupsSurfaces.FromSlug(surfaceId) is { } tab ? SetSubTabAsync(tab) : Task.CompletedTask;

    /// <summary>Switches the active sub-surface, remembers it, and puts it in the address.</summary>
    /// <param name="tab">The sub-surface to open.</param>
    internal async Task SetSubTabAsync(BackupsSubTab tab)
    {
        if (_activeSubTab == tab && _subTabRestored)
        {
            return;
        }

        _activeSubTab = tab;
        _subTabRestored = true;

        var slug = BackupsSurfaces.SlugFor(tab);

        // The address is updated in place rather than pushed, so the browser's
        // Back button leaves the area rather than walking back through every
        // surface the caller looked at.
        var route = AddressWith(Router.Current, slug);
        if (!ReferenceEquals(route, Router.Current))
        {
            Router.NavigateTo(route, replace: true);
        }

        await Preferences.SetAsync(BackupsPluginKeys.SurfacePreference, slug);
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

    // A surface segment naming one of this area's own surfaces settles the
    // question outright. A segment naming something else belongs to whatever
    // else the address is describing and is left alone.
    private bool RestoreFromAddress()
    {
        if (AddressedSurface(Router.Current) is not { } addressed)
        {
            return false;
        }

        _activeSubTab = addressed;
        _subTabRestored = true;
        return true;
    }

    /// <summary>
    /// The surface the address names, from the path segment when the address
    /// carries a selection and from this area's parameter otherwise.
    /// </summary>
    /// <remarks>
    /// Both are read rather than only the one this panel would have written,
    /// because an address can arrive from anywhere: a bookmark taken while a
    /// tree was selected, or a link built by a head that had none.
    /// </remarks>
    private static BackupsSubTab? AddressedSurface(ExplorerRoute route) =>
        BackupsSurfaces.FromSlug(route.Surface)
        ?? BackupsSurfaces.FromSlug(
            route.Parameters.GetValueOrEmpty(BackupsPluginKeys.SurfaceParameter));

    /// <summary>
    /// <paramref name="route"/> with the open surface named, in whichever half
    /// of the grammar can carry it.
    /// </summary>
    /// <remarks>
    /// The path segment is the grammar's own way to say it, but it qualifies a
    /// selection and is silently ignored without one - which is the ordinary
    /// case here, because Backups is not a selection-scoped area. Writing the
    /// parameter in that case is what keeps the surface addressable at all
    /// rather than only when a tree happens to be selected.
    /// </remarks>
    private static ExplorerRoute AddressWith(ExplorerRoute route, string slug) =>
        route.HasSelection
            ? route.WithSurface(slug)
            : route.WithParameter(BackupsPluginKeys.SurfaceParameter, slug);

    private void RestoreSubTab()
    {
        if (_subTabRestored || !Preferences.IsLoaded)
        {
            return;
        }

        _subTabRestored = true;

        // Validated against the surfaces this area actually offers, and a
        // remembered slug that resolves to none of them is forgotten rather than
        // left to be rejected again on every later visit.
        var restored = Preferences.Resolve(
            BackupsPluginKeys.SurfacePreference,
            BackupsSurfaces.New,
            state: 0,
            static (remembered, _) => BackupsSurfaces.FromSlug(remembered) is not null);

        _activeSubTab = BackupsSurfaces.FromSlug(restored.Value) ?? BackupsSubTab.New;
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
