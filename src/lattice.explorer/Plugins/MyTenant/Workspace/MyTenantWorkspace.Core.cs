using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The My Tenant plugin's view state and operations, lifted out of the panel's
/// code-behind so the six surfaces can be small components without each
/// re-deriving the state the others depend on.
/// <para>
/// Everything it does runs against its single controlled domain contract
/// (<see cref="IMyTenantDomain"/>) plus the keyed plugin access store; it holds
/// no connection, no channel, and no container (epic decision D3). That contract
/// is the tenant-administrator one: authoring quota ceilings, widening the
/// allowed region set, switching tenant, and the tenant lifecycle are not on it,
/// so this surface cannot call them at all (issue #1785).
/// </para>
/// <para>
/// <b>Every read and every mutation is scoped to one tenant:
/// <see cref="TenantId"/>.</b> That id is resolved once from the Explorer's own
/// tenant-identity seam and is never taken from a view - there is no method here
/// that accepts a tenant id - so an admin of tenant A cannot reach tenant B
/// through this surface however it is driven. Cross-tenant grants, the one place
/// a second tenant is even named, route through
/// <see cref="TenantGrantScope"/> before a call leaves the process.
/// </para>
/// </summary>
/// <remarks>
/// Client gating here is advisory (epic decision D6): the cluster re-enforces
/// every operation, so each one still folds a runtime refusal into a notice
/// rather than assuming the pre-check was the last word.
/// </remarks>
public sealed partial class MyTenantWorkspace : IDisposable
{
    private readonly IMyTenantDomain _domain;
    private readonly IExplorerPluginAccessStore _store;
    private readonly IExplorerShellPreferences? _preferences;
    private readonly IExplorerShellRouter? _router;
    private readonly Action<ExplorerRoute>? _routeChanged;

    private bool _initialized;

    /// <summary>
    /// Creates the workspace over the plugin's domain contract and the keyed
    /// access store its gate publishes into. Reads the current gate decision
    /// immediately, so a view rendered before the first probe completes is
    /// fail-closed rather than optimistic.
    /// </summary>
    /// <param name="domain">The plugin's controlled domain contract. Must not be <see langword="null"/>.</param>
    /// <param name="store">The keyed plugin access store. Must not be <see langword="null"/>.</param>
    /// <param name="preferences">
    /// The shell's preference contract, through which the open sub-surface is
    /// remembered between sessions. Optional: a head composed without it keeps
    /// the surface for the life of the circuit and no longer.
    /// </param>
    /// <param name="router">
    /// The shell's router, through which the open sub-surface is addressable.
    /// Optional: a head composed without it keeps the surface out of the URL.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="domain"/> or <paramref name="store"/> is
    /// <see langword="null"/>.
    /// </exception>
    public MyTenantWorkspace(
        IMyTenantDomain domain,
        IExplorerPluginAccessStore store,
        IExplorerShellPreferences? preferences = null,
        IExplorerShellRouter? router = null)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(store);

        _domain = domain;
        _store = store;
        _preferences = preferences;
        _router = router;

        ReadAccess();
        _store.Changed += OnAccessChanged;

        if (_router is not null)
        {
            // Bound once rather than per subscription, so unsubscribing in
            // Dispose removes the same delegate instance that was added.
            _routeChanged = OnRouteChanged;
            _router.RouteChanged += _routeChanged;
        }
    }

    /// <summary>Raised whenever the observable state changes, so the views re-render.</summary>
    public event Action? Changed;

    /// <summary>
    /// The one tenant this surface administers: the caller's active tenant, or
    /// <see langword="null"/> before it is resolved.
    /// <para>
    /// A platform operator who switched tenant sees the same surface scoped to
    /// the tenant they switched to, because the switch changes this id and
    /// nothing else about the plugin.
    /// </para>
    /// </summary>
    public string? TenantId { get; private set; }

    /// <summary>Whether a request is in flight, so every control renders disabled.</summary>
    public bool Busy { get; private set; }

    /// <summary>Whether the plugin's own gate currently admits the caller.</summary>
    public bool Allowed { get; private set; }

    /// <summary>
    /// Whether the gate refused because the connection carries no accepted
    /// credential, rather than because an authenticated caller was denied. The
    /// panel prompts a sign-in for this state instead of greying out.
    /// </summary>
    public bool AuthenticationRequired { get; private set; }

    /// <summary>
    /// Whether the deployment serves no tenancy at all, in which case the whole
    /// surface renders nothing rather than an error (epic decision D9).
    /// </summary>
    public bool Unavailable { get; private set; }

    /// <summary>The reason the gate gave, when it gave one.</summary>
    public string? AccessReason { get; private set; }

    /// <summary>
    /// What a refused caller is missing and who issues it, exactly as the gate
    /// declared it. The panel renders a denial's remedy from this rather than
    /// from the area's own label, so the reader is told which permission to ask
    /// for and whom to ask.
    /// </summary>
    public ExplorerAccessRemedy AccessRemedy { get; private set; }

    /// <summary>
    /// The refusal to render, or <see langword="null"/> when the gate admits the
    /// caller. Composed when the gate's answer changes rather than per render,
    /// and never composed at all for an allowed caller.
    /// </summary>
    public ExplorerStateMessage? AccessMessage { get; private set; }

    /// <summary>
    /// What to do about a refusal, in one sentence: the gate's own remedy when it
    /// declared one - which names the missing permission and who issues it - and
    /// the copy layer's general remedy when it did not. Never
    /// <see langword="null"/> while <see cref="AccessMessage"/> is non-null,
    /// because a refusal with no remedy is the defect this path exists to
    /// prevent.
    /// </summary>
    public string? AccessRemedyText { get; private set; }

    /// <summary>
    /// The registration-order diagnostic the gate filed, or
    /// <see langword="null"/> when the head supplied a real platform-operator
    /// gate. Rendered on the Overview surface, because a head running on the
    /// fail-closed placeholder silently loses every tenant switch and would
    /// otherwise give no clue why.
    /// </summary>
    public string? OperatorGateDiagnostic { get; private set; }

    /// <summary>The last operation's outcome, rendered as a status banner.</summary>
    public MyTenantNotice? LastNotice { get; private set; }

    /// <summary>The active internal sub-surface, one of <see cref="MyTenantSurfaces"/>'s ids.</summary>
    public string ActiveSurfaceId { get; private set; } = MyTenantSurfaces.Overview;

    /// <summary>
    /// Loads the surface: the tenant identity every other surface is scoped to,
    /// then the active surface's own data. A no-op beyond the gate read when the
    /// gate denies.
    /// </summary>
    public async Task InitializeAsync()
    {
        if (_initialized || !Allowed)
        {
            return;
        }

        _initialized = true;

        // The surface is restored first, so the load that follows is the load the
        // restored surface needs rather than the default one's.
        await RestoreSurfaceAsync().ConfigureAwait(false);
        await LoadIdentityAsync().ConfigureAwait(false);
        await LoadForSurfaceAsync(force: true).ConfigureAwait(false);
        RaiseChanged();
    }

    /// <summary>
    /// Activates <paramref name="surfaceId"/>, clearing the previous surface's
    /// notice and loading the newly activated surface's data if it has not been
    /// loaded yet, then remembers it and puts it in the address.
    /// </summary>
    /// <param name="surfaceId">The sub-surface id to activate.</param>
    public async Task SelectSurfaceAsync(string surfaceId)
    {
        if (!await ActivateSurfaceAsync(surfaceId).ConfigureAwait(false))
        {
            return;
        }

        await RememberSurfaceAsync(surfaceId, replaceHistoryEntry: false).ConfigureAwait(false);
    }

    /// <summary>
    /// Activates <paramref name="surfaceId"/> without persisting or addressing
    /// it, and reports whether it actually changed. The half a restore and a
    /// browser Back share with an explicit tab click.
    /// </summary>
    private async Task<bool> ActivateSurfaceAsync(string surfaceId)
    {
        if (!MyTenantSurfaces.IsKnown(surfaceId)
            || string.Equals(ActiveSurfaceId, surfaceId, StringComparison.Ordinal))
        {
            return false;
        }

        ActiveSurfaceId = surfaceId;
        LastNotice = null;

        await LoadForSurfaceAsync(force: false).ConfigureAwait(false);
        RaiseChanged();
        return true;
    }

    /// <summary>
    /// Re-reads the tenant identity and reloads the active surface. The area's
    /// Refresh command.
    /// </summary>
    public async Task ReloadAsync()
    {
        if (!Allowed)
        {
            return;
        }

        LastNotice = null;
        await LoadIdentityAsync().ConfigureAwait(false);
        await LoadForSurfaceAsync(force: true).ConfigureAwait(false);
        RaiseChanged();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _store.Changed -= OnAccessChanged;

        if (_router is not null && _routeChanged is not null)
        {
            _router.RouteChanged -= _routeChanged;
        }
    }

    /// <summary>
    /// Runs one mutation under the busy flag, recording its outcome as the
    /// surface's notice and applying <paramref name="onSuccess"/> to the value
    /// the cluster committed before the re-render, so the surface never shows a
    /// stale list beside a fresh success banner.
    /// </summary>
    /// <remarks>
    /// A cancellation the caller asked for is left to propagate, exactly as the
    /// operations seam documents; only outcomes the cluster produced become
    /// notices.
    /// </remarks>
    private async Task<bool> RunAsync<TValue>(
        Func<Task<TenantOperationResult<TValue>>> operation,
        Action<TValue>? onSuccess = null)
    {
        if (Busy)
        {
            return false;
        }

        Busy = true;
        RaiseChanged();
        try
        {
            var result = await operation().ConfigureAwait(false);
            LastNotice = MyTenantNotice.For(result);

            if (result.IsSuccess && result.Value is { } value)
            {
                onSuccess?.Invoke(value);
            }

            return result.IsSuccess;
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>
    /// Records a refusal the plugin made itself, before any call left the
    /// process, and re-renders.
    /// </summary>
    private void Refuse(TenantOperationStatus status, string message, string? guidance = null)
    {
        LastNotice = MyTenantNotice.Refused(status, message, guidance);
        RaiseChanged();
    }

    private async Task LoadForSurfaceAsync(bool force)
    {
        switch (ActiveSurfaceId)
        {
            case MyTenantSurfaces.Overview:
                await LoadOverviewAsync(force).ConfigureAwait(false);
                break;
            case MyTenantSurfaces.Members:
                await LoadAdminSubjectsAsync(force).ConfigureAwait(false);
                break;
            case MyTenantSurfaces.Quota:
                await LoadQuotaAsync(force).ConfigureAwait(false);
                break;
            case MyTenantSurfaces.Regions:
                await LoadRegionsAsync(force).ConfigureAwait(false);
                break;
            case MyTenantSurfaces.Sharing:
                await LoadGrantsAsync(force).ConfigureAwait(false);
                break;
            default:
                // The Metrics surface has no data of its own yet; its section
                // seam loads whatever it needs itself.
                break;
        }
    }

    private void ReadAccess()
    {
        var access = _store.Get(MyTenantPluginKeys.PluginId);
        Allowed = access.IsAllowed;
        AuthenticationRequired = access.State == ExplorerPluginAccessState.AuthenticationRequired;
        Unavailable = access.State == ExplorerPluginAccessState.Unavailable;
        AccessReason = access.Reason;
        AccessRemedy = access.Remedy;

        // Composed here, when the gate's answer changes, rather than on the
        // render path: the panel re-renders on every load and every notice, and
        // an allowed caller composes nothing at all.
        AccessMessage = ExplorerAccessCopy.For(
            ExplorerVocabulary.MyTenantArea,
            access.IsAllowed,
            requiresSignIn: AuthenticationRequired,
            isUnavailable: Unavailable);

        // The gate names the missing permission and its audience; the copy layer
        // knows only the surface label. Prefer the gate, and fall back to the
        // copy layer's general remedy rather than to nothing.
        AccessRemedyText = AccessMessage is null
            ? null
            : access.Remedy.Describe() ?? AccessMessage.Remedy;

        var diagnostic = _store.Get(MyTenantPluginKeys.PluginId, MyTenantPluginKeys.OperatorGateScope);
        OperatorGateDiagnostic = diagnostic.IsAllowed ? null : diagnostic.Reason;
    }

    private void OnAccessChanged(ExplorerPluginAccessChange change)
    {
        // Only this plugin's own keys matter: a sibling plugin's probe completing
        // must not re-render this one.
        if (!string.Equals(change.Key.PluginId, MyTenantPluginKeys.PluginId, StringComparison.Ordinal))
        {
            return;
        }

        var wasAllowed = Allowed;
        ReadAccess();

        // A gate that has just opened for the first time wants its data loaded
        // without the caller reaching for Refresh. An already-initialized surface
        // falls through to the re-render instead: InitializeAsync short-circuits
        // on _initialized and would announce nothing, so a gate that closed and
        // re-opened - a token expiring, then a sign-in - would otherwise leave
        // the denied message on screen with the data already in hand.
        if (!wasAllowed && Allowed && !_initialized)
        {
            _ = InitializeAsync();
            return;
        }

        RaiseChanged();
    }

    private void RaiseChanged() => Changed?.Invoke();
}
