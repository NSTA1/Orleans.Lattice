using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

/// <summary>
/// The Tenants plugin's view state and operations, lifted out of the panel's
/// code-behind so the surface can be split into small views by concern without
/// each one re-deriving the state the others depend on - and so every rule that
/// matters here is exercisable without rendering a component.
/// <para>
/// Everything the plugin does runs against its single controlled domain contract
/// (<see cref="ITenancyDomain"/>) plus the keyed plugin access store; it holds no
/// connection, no channel, and no container (epic decision D3). Every action is
/// gated on the plugin's own advisory access decision, read from the store under
/// <see cref="TenantsPluginKeys.PluginId"/> - rendering disabled, not hidden,
/// when denied - and folds a server refusal into a specific status banner rather
/// than surfacing an unhandled error.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// Gating here is advisory: the cluster remains the sole enforcement point, so
/// every operation still handles a runtime refusal (epic decision D6).
/// </para>
/// <para>
/// Every destructive operation - delete, suspend, admin-subject removal, grant
/// revocation and rejection, and a region revocation that would strand residency
/// - is requested, held as a <see cref="TenantConfirmation"/>, and performed only
/// on an explicit confirm. None of them can fire from the click that asks for
/// them.
/// </para>
/// </remarks>
public sealed partial class TenantsWorkspace : IDisposable
{
    /// <summary>
    /// How many tenants one page of the list holds. The control API returns the
    /// accessible tenants in one call, so paging is a display concern: it bounds
    /// the rows rendered, and bounds the per-tenant usage reads to one page's
    /// worth rather than the whole cluster's.
    /// </summary>
    public const int PageSize = 20;

    private static readonly IReadOnlyList<ExplorerTenantSummary> NoTenants =
        Array.Empty<ExplorerTenantSummary>();

    private readonly ITenancyDomain _domain;
    private readonly IExplorerPluginAccessStore _store;

    // Reused across page changes and reloads: the list re-renders on every gate
    // change and every busy transition, and rebuilding a fresh collection each
    // time would allocate one per render rather than one per data change.
    private readonly List<TenantRow> _page = new(PageSize);

    // Headline usage per tenant, read once per tenant per reload and reused
    // while paging back and forth. Cleared on reload so a stale figure cannot
    // outlive the list it belongs to.
    private readonly Dictionary<string, ExplorerTenantQuotaUsage> _headlineUsage =
        new(StringComparer.Ordinal);

    private IReadOnlyList<ExplorerTenantSummary> _tenants = NoTenants;

    /// <summary>
    /// Creates the workspace over the plugin's domain contract and the keyed
    /// access store its gate publishes into. Reads the current gate decision
    /// immediately, so a view rendered before the first probe completes is
    /// fail-closed rather than optimistic.
    /// </summary>
    /// <param name="domain">The plugin's controlled domain contract. Must not be <see langword="null"/>.</param>
    /// <param name="store">The keyed plugin access store. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public TenantsWorkspace(ITenancyDomain domain, IExplorerPluginAccessStore store)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(store);

        _domain = domain;
        _store = store;

        ReadAccess();
        _store.Changed += OnAccessChanged;
    }

    /// <summary>Raised whenever the observable state changes, so the views re-render.</summary>
    public event Action? Changed;

    /// <summary>Whether the plugin's own gate currently admits the caller as a platform operator.</summary>
    public bool Allowed { get; private set; }

    /// <summary>
    /// Whether the gate refused because the connection carries no accepted
    /// credential, rather than because an authenticated caller was refused. The
    /// panel prompts a sign-in for this state instead of greying out.
    /// </summary>
    public bool AuthenticationRequired { get; private set; }

    /// <summary>
    /// Whether the cluster serves no tenant administration at all. The shell
    /// renders no Tenants entry in this state, so the panel is not normally
    /// reached; it still degrades to a single explanatory line rather than an
    /// empty surface if a head renders it directly (epic decision D9).
    /// </summary>
    public bool Unavailable { get; private set; }

    /// <summary>
    /// The advisory reason the gate gave, or <see langword="null"/> when it gave
    /// none. Display text only.
    /// </summary>
    public string? AccessReason { get; private set; }

    /// <summary>Whether a request is in flight, so every action renders disabled.</summary>
    public bool Busy { get; private set; }

    /// <summary>
    /// The last operation's outcome classification, or <see langword="null"/>
    /// before the first operation.
    /// </summary>
    public TenantOperationStatus? LastStatus { get; private set; }

    /// <summary>
    /// The last operation's outcome as a sentence, or <see langword="null"/>
    /// before the first operation. Already carries the specific meaning of a
    /// refusal rather than a generic failure.
    /// </summary>
    public string? LastMessage { get; private set; }

    /// <summary>The status-banner modifier class for <see cref="LastStatus"/>.</summary>
    public string LastResultClass =>
        LastStatus is { } status ? TenantRefusal.ResultClass(status) : string.Empty;

    /// <summary>The active internal sub-surface, one of <see cref="TenantsSurfaces"/>'s ids.</summary>
    public string ActiveSurfaceId { get; private set; } = TenantsSurfaces.Tenants;

    /// <summary>
    /// The current page of the tenant list, in the order the cluster returned.
    /// The same instance across renders, refreshed in place when the data or the
    /// page changes.
    /// </summary>
    public IReadOnlyList<TenantRow> Page => _page;

    /// <summary>The number of tenants the caller can see.</summary>
    public int TenantCount => _tenants.Count;

    /// <summary>The zero-based index of the page currently in view.</summary>
    public int PageIndex { get; private set; }

    /// <summary>
    /// The number of pages the list spans, at least one so an empty list still
    /// reads as "page 1 of 1" rather than "page 1 of 0".
    /// </summary>
    public int PageCount => Math.Max(1, (_tenants.Count + PageSize - 1) / PageSize);

    /// <summary>Whether a page precedes the one in view.</summary>
    public bool HasPreviousPage => PageIndex > 0;

    /// <summary>Whether a page follows the one in view.</summary>
    public bool HasNextPage => PageIndex + 1 < PageCount;

    /// <summary>
    /// The tenant the tenant-scoped sub-surfaces are showing, or
    /// <see langword="null"/> when none is selected.
    /// </summary>
    public string? SelectedTenantId { get; private set; }

    /// <summary>
    /// The selected tenant's lifecycle state, residency, and authored quota
    /// ceilings, or <see langword="null"/> when none is selected or the read was
    /// refused.
    /// </summary>
    public ExplorerTenantDetail? SelectedDetail { get; private set; }

    /// <summary>
    /// Whether the selected tenant is the reserved default, which cannot be
    /// suspended, deleted, or have its admin subjects or cross-tenant grants
    /// edited. The controls that would do so render disabled, and the cluster
    /// refuses them regardless.
    /// </summary>
    public bool SelectedIsDefault => SelectedDetail?.IsDefault ?? false;

    /// <summary>
    /// Loads the surface: the tenant list and the first page's headline usage. A
    /// no-op beyond the gate read when the gate does not admit the caller.
    /// </summary>
    public Task InitializeAsync() => ReloadAsync();

    /// <summary>
    /// Reloads the tenant list from the cluster, discarding cached headline
    /// usage and re-reading the selected tenant if it is still visible.
    /// </summary>
    public async Task ReloadAsync()
    {
        ClearResult();
        if (!Allowed || Busy)
        {
            RaiseChanged();
            return;
        }

        BeginBusy();
        try
        {
            var listed = await _domain.Tenants.ListAccessibleTenantsAsync().ConfigureAwait(false);
            if (!listed.IsSuccess)
            {
                _tenants = NoTenants;
                _headlineUsage.Clear();
                _page.Clear();
                Report(listed);
                return;
            }

            _tenants = listed.Value ?? NoTenants;
            _headlineUsage.Clear();
            PageIndex = Math.Clamp(PageIndex, 0, PageCount - 1);

            await LoadPageUsageAsync().ConfigureAwait(false);
            RebuildPage();

            // A selection that survived the reload is re-read so its detail,
            // quotas, regions, and access lists are not left describing the
            // tenant as it was before.
            if (SelectedTenantId is { } selected && ContainsTenant(selected))
            {
                await LoadSelectedAsync(selected).ConfigureAwait(false);
            }
            else
            {
                ClearSelection();
            }
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Activates <paramref name="surfaceId"/> and loads its data if it has not
    /// been loaded for the selected tenant yet.
    /// </summary>
    /// <param name="surfaceId">The sub-surface id to activate.</param>
    public async Task SelectSurfaceAsync(string surfaceId)
    {
        ArgumentNullException.ThrowIfNull(surfaceId);

        if (Busy || string.Equals(ActiveSurfaceId, surfaceId, StringComparison.Ordinal))
        {
            return;
        }

        ActiveSurfaceId = surfaceId;
        ClearResult();

        // Leaving a surface drops any pending confirmation, so an operator
        // cannot navigate away and later confirm a destructive action they have
        // lost the context for.
        Confirmation = null;

        // Marked busy for the load, so an action on the newly activated surface
        // cannot be started against data that has not arrived yet.
        BeginBusy();
        try
        {
            await LoadForSurfaceAsync(force: false).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Selects <paramref name="tenantId"/> and loads its detail plus the active
    /// sub-surface's data.
    /// </summary>
    /// <param name="tenantId">The tenant to select.</param>
    public async Task SelectTenantAsync(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);

        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        Confirmation = null;

        BeginBusy();
        try
        {
            await LoadSelectedAsync(tenantId).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>Moves to the next page of the tenant list, if there is one.</summary>
    public Task NextPageAsync() => GoToPageAsync(PageIndex + 1);

    /// <summary>Moves to the previous page of the tenant list, if there is one.</summary>
    public Task PreviousPageAsync() => GoToPageAsync(PageIndex - 1);

    /// <inheritdoc />
    public void Dispose() => _store.Changed -= OnAccessChanged;

    private async Task GoToPageAsync(int pageIndex)
    {
        if (Busy || pageIndex < 0 || pageIndex >= PageCount || pageIndex == PageIndex)
        {
            return;
        }

        PageIndex = pageIndex;
        BeginBusy();
        try
        {
            await LoadPageUsageAsync().ConfigureAwait(false);
            RebuildPage();
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task LoadSelectedAsync(string tenantId)
    {
        SelectedTenantId = tenantId;

        var detail = await _domain.Tenants.GetTenantAsync(tenantId).ConfigureAwait(false);
        if (!detail.IsSuccess)
        {
            SelectedDetail = null;
            Report(detail);
            return;
        }

        SelectedDetail = detail.Value;
        ResetTenantScopedSurfaces();
        await LoadForSurfaceAsync(force: true).ConfigureAwait(false);
    }

    private Task LoadForSurfaceAsync(bool force) => ActiveSurfaceId switch
    {
        TenantsSurfaces.Quotas => LoadQuotasAsync(force),
        TenantsSurfaces.Regions => LoadRegionsAsync(force),
        TenantsSurfaces.Access => LoadAccessAsync(force),
        _ => Task.CompletedTask,
    };

    /// <summary>
    /// Reads headline usage for the tenants on the page in view, skipping any
    /// already cached. Bounded by the page, so the cluster is asked for at most
    /// <see cref="PageSize"/> readings however many tenants exist, and a refusal
    /// on one tenant leaves the others intact - the row simply reports that its
    /// usage was not read rather than showing a fabricated zero.
    /// </summary>
    private async Task LoadPageUsageAsync()
    {
        var start = PageIndex * PageSize;
        var end = Math.Min(start + PageSize, _tenants.Count);

        List<string>? ids = null;
        List<Task<TenantOperationResult<ExplorerTenantQuotaUsage>>>? reads = null;
        for (var i = start; i < end; i++)
        {
            var tenantId = _tenants[i].TenantId;
            if (_headlineUsage.ContainsKey(tenantId))
            {
                continue;
            }

            // Allocated only when there is uncached work, so paging back over
            // already-read tenants costs nothing.
            ids ??= new List<string>(end - start);
            reads ??= new List<Task<TenantOperationResult<ExplorerTenantQuotaUsage>>>(end - start);
            ids.Add(tenantId);
            reads.Add(_domain.Tenants.GetQuotaUsageAsync(tenantId));
        }

        if (reads is null || ids is null)
        {
            return;
        }

        var readings = await Task.WhenAll(reads).ConfigureAwait(false);
        for (var i = 0; i < readings.Length; i++)
        {
            if (readings[i].IsSuccess && readings[i].Value is { } usage)
            {
                _headlineUsage[ids[i]] = usage;
            }
        }
    }

    private void RebuildPage()
    {
        _page.Clear();

        var start = PageIndex * PageSize;
        var end = Math.Min(start + PageSize, _tenants.Count);
        for (var i = start; i < end; i++)
        {
            var summary = _tenants[i];
            _headlineUsage.TryGetValue(summary.TenantId, out var usage);
            _page.Add(TenantRow.From(summary, usage));
        }
    }

    private bool ContainsTenant(string tenantId)
    {
        for (var i = 0; i < _tenants.Count; i++)
        {
            if (string.Equals(_tenants[i].TenantId, tenantId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private void ClearSelection()
    {
        SelectedTenantId = null;
        SelectedDetail = null;
        ResetTenantScopedSurfaces();
    }

    private void ReadAccess()
    {
        var access = _store.Get(TenantsPluginKeys.PluginId);
        Allowed = access.IsAllowed;
        AuthenticationRequired = access.State == ExplorerPluginAccessState.AuthenticationRequired;
        Unavailable = access.State == ExplorerPluginAccessState.Unavailable;
        AccessReason = access.Reason;
    }

    private void OnAccessChanged(ExplorerPluginAccessChange change)
    {
        // Only this plugin's own plugin-level decision gates the panel; a sibling
        // plugin's probe completing must not re-render it.
        if (change.Key.Scope is not null
            || !string.Equals(change.Key.PluginId, TenantsPluginKeys.PluginId, StringComparison.Ordinal))
        {
            return;
        }

        var wasAllowed = Allowed;
        ReadAccess();

        // A gate that freshly opens - after the connection reaches the cluster or
        // an operator signs in - populates the list without a manual refresh.
        if (!wasAllowed && Allowed && _tenants.Count == 0)
        {
            _ = ReloadAsync();
            return;
        }

        RaiseChanged();
    }

    private void BeginBusy()
    {
        Busy = true;
        RaiseChanged();
    }

    private void EndBusy()
    {
        Busy = false;
        RaiseChanged();
    }

    private void Report(TenantOperationResult result)
    {
        LastStatus = result.Status;
        LastMessage = TenantRefusal.Describe(result);
    }

    private void Report(TenantOperationStatus status, string message)
    {
        LastStatus = status;
        LastMessage = message;
    }

    private void ClearResult()
    {
        LastStatus = null;
        LastMessage = null;
    }

    private void RaiseChanged() => Changed?.Invoke();
}
