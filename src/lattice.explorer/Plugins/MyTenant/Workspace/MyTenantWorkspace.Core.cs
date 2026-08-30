using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The My Tenant plugin's view state and operations, lifted out of the panel's
/// code-behind so the six surfaces can be small components without each
/// re-deriving the state the others depend on.
/// <para>
/// Everything it does runs against its single controlled domain contract
/// (<see cref="ITenancyDomain"/>) plus the keyed plugin access store; it holds no
/// connection, no channel, and no container (epic decision D3).
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
    private readonly ITenancyDomain _domain;
    private readonly IExplorerPluginAccessStore _store;

    private bool _initialized;

    /// <summary>
    /// Creates the workspace over the plugin's domain contract and the keyed
    /// access store its gate publishes into. Reads the current gate decision
    /// immediately, so a view rendered before the first probe completes is
    /// fail-closed rather than optimistic.
    /// </summary>
    /// <param name="domain">The plugin's controlled domain contract. Must not be <see langword="null"/>.</param>
    /// <param name="store">The keyed plugin access store. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public MyTenantWorkspace(ITenancyDomain domain, IExplorerPluginAccessStore store)
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
        await LoadIdentityAsync().ConfigureAwait(false);
        await LoadForSurfaceAsync(force: true).ConfigureAwait(false);
        RaiseChanged();
    }

    /// <summary>
    /// Activates <paramref name="surfaceId"/>, clearing the previous surface's
    /// notice and loading the newly activated surface's data if it has not been
    /// loaded yet.
    /// </summary>
    /// <param name="surfaceId">The sub-surface id to activate.</param>
    public async Task SelectSurfaceAsync(string surfaceId)
    {
        if (!MyTenantSurfaces.IsKnown(surfaceId)
            || string.Equals(ActiveSurfaceId, surfaceId, StringComparison.Ordinal))
        {
            return;
        }

        ActiveSurfaceId = surfaceId;
        LastNotice = null;

        await LoadForSurfaceAsync(force: false).ConfigureAwait(false);
        RaiseChanged();
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
    public void Dispose() => _store.Changed -= OnAccessChanged;

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

        // A gate that has just opened wants its data loaded without the caller
        // reaching for Refresh; anything else is a re-render.
        if (!wasAllowed && Allowed)
        {
            _ = InitializeAsync();
            return;
        }

        RaiseChanged();
    }

    private void RaiseChanged() => Changed?.Invoke();
}
