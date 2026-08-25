namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantSwitcher"/>. Reads the current scope from
/// the per-circuit <see cref="IExplorerTenantContext"/> and the active
/// <see cref="IExplorerTenantView"/>, and applies operator-only mutations after
/// validating the caller through <see cref="IExplorerTenantOperatorGate"/>. Every
/// mutation is fail-closed: a denied or anonymous caller changes nothing, so a
/// non-operator can never switch tenant or self-elevate to the cross-tenant view.
/// </summary>
internal sealed class ExplorerTenantSwitcher(
    IExplorerTenantView view,
    IExplorerTenantContext context,
    IExplorerTenantOperatorGate operatorGate) : IExplorerTenantSwitcher
{
    private readonly IExplorerTenantView _view =
        view ?? throw new ArgumentNullException(nameof(view));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private readonly IExplorerTenantOperatorGate _operatorGate =
        operatorGate ?? throw new ArgumentNullException(nameof(operatorGate));

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
        return true;
    }
}
