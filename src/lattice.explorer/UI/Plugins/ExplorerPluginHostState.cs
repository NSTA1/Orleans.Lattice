using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The head's side of the plugin host-state seam: it adapts the Explorer's own
/// <see cref="IExplorerSelection"/>, <see cref="ILatticeStateConnection"/> and
/// <see cref="IExplorerTenantView"/> onto the plugin contract's narrow
/// projections.
/// <para>
/// The adaptation lives here, and not in the contract package, precisely so the
/// contract can carry no cluster dependency: a plugin cannot be handed the
/// connection because nothing it can see expresses one. Everything crossing the
/// seam is a value projection - a selection id and label, a connection state, a
/// tenant scope - never a live Explorer service.
/// </para>
/// </summary>
public sealed class ExplorerPluginHostState : IExplorerPluginHostState, IDisposable
{
    private readonly IExplorerSelection _selection;
    private readonly ILatticeStateConnection _connection;
    private readonly IExplorerTenantView _tenants;

    // Projected once per upstream transition rather than per read, so the render
    // path and every gate probe read a field instead of rebuilding a projection.
    private ExplorerPluginSelection? _projectedSelection;
    private ExplorerPluginConnectionStatus _projectedConnection;
    private ExplorerPluginTenantScope _projectedTenant;
    private bool _disposed;

    /// <summary>
    /// Binds the adapter to the Explorer services it projects and subscribes to
    /// their change notifications.
    /// </summary>
    /// <param name="selection">The Explorer's current selection. Must not be <see langword="null"/>.</param>
    /// <param name="connection">The Explorer's shared state-API connection. Must not be <see langword="null"/>.</param>
    /// <param name="tenants">
    /// The Explorer's tenant view. Pass <see langword="null"/> on a deployment
    /// with no tenancy add-on; the adapter then reports the inactive scope, which
    /// is what a tenancy plugin's gate reads as unavailable.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="selection"/> or <paramref name="connection"/> is <see langword="null"/>.</exception>
    public ExplorerPluginHostState(
        IExplorerSelection selection,
        ILatticeStateConnection connection,
        IExplorerTenantView? tenants = null)
    {
        ArgumentNullException.ThrowIfNull(selection);
        ArgumentNullException.ThrowIfNull(connection);

        _selection = selection;
        _connection = connection;
        _tenants = tenants ?? InactiveTenantView.Instance;

        _projectedSelection = Project(_selection.Selected);
        _projectedConnection = Project(_connection.Status);
        _projectedTenant = ProjectTenant(ExplorerPluginTenantVisibility.ActiveTenant);

        _selection.SelectionChanged += OnSelectionChanged;
        _connection.StatusChanged += OnConnectionChanged;
    }

    /// <inheritdoc />
    public event Action<ExplorerPluginHostChange>? Changed;

    /// <inheritdoc />
    public ExplorerPluginSelection? Selection => _projectedSelection;

    /// <inheritdoc />
    public ExplorerPluginConnectionStatus Connection => _projectedConnection;

    /// <inheritdoc />
    public ExplorerPluginTenantScope Tenant => _projectedTenant;

    /// <summary>
    /// Re-resolves the tenant scope from the tenant view and republishes it when
    /// it changed. The host calls this on the same occasions it re-probes the
    /// plugin gates (mount, sign-in change, reconnect), so the published scope is
    /// refreshed deterministically rather than on a background timer.
    /// <para>
    /// Fail-closed and fault-isolated: a tenant view that throws leaves the scope
    /// at the active-tenant default rather than widening it, and never propagates
    /// to the caller.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the visibility resolution.</param>
    public async Task RefreshTenantScopeAsync(CancellationToken cancellationToken = default)
    {
        ExplorerPluginTenantVisibility visibility;
        try
        {
            var resolved = await _tenants
                .ResolveEffectiveVisibilityAsync(cancellationToken)
                .ConfigureAwait(false);
            visibility = resolved == ExplorerTenantVisibility.AllTenants
                ? ExplorerPluginTenantVisibility.AllTenants
                : ExplorerPluginTenantVisibility.ActiveTenant;
        }
        catch (Exception)
        {
            // An unresolvable visibility is never an admission: degrade to the
            // caller's own tenant, exactly as an unvalidated cross-tenant request
            // does.
            visibility = ExplorerPluginTenantVisibility.ActiveTenant;
        }

        Publish(ProjectTenant(visibility));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _selection.SelectionChanged -= OnSelectionChanged;
        _connection.StatusChanged -= OnConnectionChanged;
    }

    private void OnSelectionChanged()
    {
        var projected = Project(_selection.Selected);
        if (projected == _projectedSelection)
        {
            return;
        }

        _projectedSelection = projected;
        Changed?.Invoke(ExplorerPluginHostChange.Selection);
    }

    private void OnConnectionChanged(LatticeConnectionStatus status)
    {
        var projected = Project(status);
        if (projected == _projectedConnection)
        {
            return;
        }

        _projectedConnection = projected;
        Changed?.Invoke(ExplorerPluginHostChange.Connection);
    }

    private void Publish(ExplorerPluginTenantScope scope)
    {
        if (scope == _projectedTenant)
        {
            return;
        }

        _projectedTenant = scope;
        Changed?.Invoke(ExplorerPluginHostChange.Tenant);
    }

    private ExplorerPluginTenantScope ProjectTenant(ExplorerPluginTenantVisibility visibility) =>
        new(_tenants.IsActive, _tenants.ActiveTenant?.Value, visibility);

    private static ExplorerPluginSelection? Project(CatalogItem? item) => item is null
        ? null
        : new ExplorerPluginSelection
        {
            Id = item.Id,
            Label = item.Label,
            Kind = ExplorerSelectionKindProjection.ToPluginKind(item.Kind),
        };

    private static ExplorerPluginConnectionStatus Project(LatticeConnectionStatus status) => new(
        status.State switch
        {
            LatticeConnectionState.Connecting => ExplorerPluginConnectionState.Connecting,
            LatticeConnectionState.Connected => ExplorerPluginConnectionState.Connected,
            LatticeConnectionState.Reconnecting => ExplorerPluginConnectionState.Reconnecting,
            LatticeConnectionState.Faulted => ExplorerPluginConnectionState.Faulted,
            _ => ExplorerPluginConnectionState.Disconnected,
        },
        status.RequiresAuthentication);

    /// <summary>
    /// The inactive stand-in used when the head registered no tenant view, so
    /// the adapter needs no null check on its read path.
    /// </summary>
    private sealed class InactiveTenantView : IExplorerTenantView
    {
        public static InactiveTenantView Instance { get; } = new();

        public bool IsActive => false;

        public ExplorerTenantId? ActiveTenant => null;

        public ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(ExplorerTenantVisibility.ActiveTenant);

        public bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId) => true;

        public ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
            IReadOnlyList<TItem> items,
            Func<TItem, string> treeIdSelector,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(items);
    }
}
