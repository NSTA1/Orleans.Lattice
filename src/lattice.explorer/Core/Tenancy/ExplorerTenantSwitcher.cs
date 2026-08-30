namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantSwitcher"/>. Reads the current scope from
/// the per-circuit <see cref="IExplorerTenantContext"/> and the active
/// <see cref="IExplorerTenantView"/>, and applies operator-only mutations after
/// validating the caller through <see cref="IExplorerTenantOperatorGate"/>. Every
/// mutation is fail-closed: a denied or anonymous caller changes nothing, so a
/// non-operator can never switch tenant or self-elevate to the cross-tenant view.
/// </summary>
/// <remarks>
/// An applied mutation is a <em>refresh occasion</em>: it raises
/// <see cref="IExplorerTenantScopeRefresher"/> so whatever the host derives from
/// the caller's tenant scope is re-resolved, exactly as it is on mount, on a
/// sign-in change, and on a reconnect. The refresher is optional, so a
/// deployment that registers none behaves as before.
/// </remarks>
internal sealed class ExplorerTenantSwitcher(
    IExplorerTenantView view,
    IExplorerTenantContext context,
    IExplorerTenantOperatorGate operatorGate,
    IExplorerTenantScopeRefresher? scopeRefresher = null) : IExplorerTenantSwitcher
{
    private readonly IExplorerTenantView _view =
        view ?? throw new ArgumentNullException(nameof(view));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private readonly IExplorerTenantOperatorGate _operatorGate =
        operatorGate ?? throw new ArgumentNullException(nameof(operatorGate));

    private readonly IExplorerTenantScopeRefresher? _scopeRefresher = scopeRefresher;

    /// <inheritdoc />
    public bool IsActive => _view.IsActive;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant => _view.ActiveTenant;

    /// <inheritdoc />
    public ExplorerTenantVisibility RequestedVisibility => _context.RequestedVisibility;

    /// <inheritdoc />
    public ValueTask<bool> IsOperatorAsync(CancellationToken cancellationToken = default)
    {
        // Inactive view: nothing renders and no caller is an operator here.
        if (!_view.IsActive)
        {
            return new ValueTask<bool>(false);
        }

        return _operatorGate.IsPlatformOperatorAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default)
    {
        if (!await IsOperatorAsync(cancellationToken).ConfigureAwait(false))
        {
            // Fail-closed: a non-operator (or the inactive view) never changes the
            // requested scope, so it cannot enable the all-tenant view.
            return false;
        }

        _context.RequestedVisibility = visibility;
        await RefreshScopeAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default)
    {
        if (!await IsOperatorAsync(cancellationToken).ConfigureAwait(false))
        {
            // Fail-closed: a non-operator (or the inactive view) never re-targets
            // the active tenant, so it cannot reach another tenant's data.
            return false;
        }

        _context.ActiveTenant = tenant;
        await RefreshScopeAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Tells the host the caller's tenant scope changed, so everything derived
    /// from it is re-resolved rather than left describing the scope the caller
    /// just left.
    /// </summary>
    /// <remarks>
    /// Faults are contained and never reported to the caller: the mutation above
    /// has already been applied to the per-circuit context, so returning
    /// <see langword="false"/> because a downstream refresh failed would claim
    /// the switch did not happen when it did. The refresh itself is fail-closed,
    /// and the cluster re-enforces on every call regardless, so a missed refresh
    /// costs a stale projection rather than an admission.
    /// </remarks>
    private async ValueTask RefreshScopeAsync(CancellationToken cancellationToken)
    {
        if (_scopeRefresher is null)
        {
            return;
        }

        try
        {
            await _scopeRefresher.RefreshAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Presentation-layer refresh: never let it fail an applied mutation.
        }
    }
}
