using Orleans.Lattice.Explorer.Core.Session;

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
/// <para>
/// An applied mutation is a <em>refresh occasion</em>: it raises
/// <see cref="IExplorerTenantScopeRefresher"/> so whatever the host derives from
/// the caller's tenant scope is re-resolved, exactly as it is on mount, on a
/// sign-in change, and on a reconnect. The refresher is optional, so a
/// deployment that registers none behaves as before.
/// </para>
/// <para>
/// <b>Every outcome is stated, not just enacted.</b> Each mutation publishes an
/// <see cref="ExplorerTenantScopeNotice"/> - applied, or refused - so the shell
/// announces it. The fail-closed <see cref="bool"/> results used to be discarded
/// by the only caller, which is why a genuine denial looked exactly like a switch
/// that worked: nothing happened and nothing was said. Publishing here rather
/// than at the call site means <em>every</em> caller reports honestly, including
/// a tenant list offering "set as active tenant".
/// </para>
/// <para>
/// <b>An applied scope is remembered.</b> A successful mutation is written
/// through the shell's preference contract, scoped per user and per cluster, so
/// the caller returns to the same scope in a later session rather than only for
/// the rest of this page. Persistence never fails a mutation that has already
/// been applied: the durable store is a convenience, and the context is the
/// authority for this circuit.
/// </para>
/// </remarks>
internal sealed class ExplorerTenantSwitcher(
    IExplorerTenantView view,
    IExplorerTenantContext context,
    IExplorerTenantOperatorGate operatorGate,
    IExplorerTenantScopeRefresher? scopeRefresher = null,
    IExplorerShellPreferences? preferences = null,
    IExplorerTenantScopeNotices? notices = null) : IExplorerTenantSwitcher
{
    private readonly IExplorerTenantView _view =
        view ?? throw new ArgumentNullException(nameof(view));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private readonly IExplorerTenantOperatorGate _operatorGate =
        operatorGate ?? throw new ArgumentNullException(nameof(operatorGate));

    private readonly IExplorerTenantScopeRefresher? _scopeRefresher = scopeRefresher;
    private readonly IExplorerShellPreferences? _preferences = preferences;
    private readonly IExplorerTenantScopeNotices? _notices = notices;

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
            Refuse();
            return false;
        }

        _context.RequestedVisibility = visibility;

        var allTenants = visibility == ExplorerTenantVisibility.AllTenants;
        _notices?.Publish(ExplorerTenantScopeNotice.VisibilityApplied(allTenants));
        await RememberAsync(ExplorerPreferenceKeys.AllTenantsVisible, allTenants, cancellationToken)
            .ConfigureAwait(false);
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
            Refuse();
            return false;
        }

        _context.ActiveTenant = tenant;

        _notices?.Publish(ExplorerTenantScopeNotice.Applied(tenant));
        await RememberAsync(ExplorerPreferenceKeys.ActiveTenant, tenant.Value, cancellationToken)
            .ConfigureAwait(false);
        await RefreshScopeAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// States the fail-closed refusal, so a denial is explained to the caller
    /// rather than merely leaving the scope unchanged.
    /// </summary>
    private void Refuse() => _notices?.Publish(ExplorerTenantScopeNotice.Refused());

    /// <summary>
    /// Remembers an applied scope through the shell's preference contract, so it
    /// is re-selected in a later session and not merely for the rest of this one.
    /// </summary>
    /// <remarks>
    /// Faults are contained for the same reason as
    /// <see cref="RefreshScopeAsync"/>: the mutation is already applied to the
    /// per-circuit context, so failing the call because a durable write did not
    /// land would claim the switch did not happen when it did. A prerender pass
    /// cannot reach browser storage at all, and that is not an error either.
    /// </remarks>
    private async ValueTask RememberAsync<T>(
        ExplorerPreferenceKey key,
        T value,
        CancellationToken cancellationToken)
    {
        if (_preferences is null)
        {
            return;
        }

        try
        {
            await _preferences.EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);
            await _preferences.SetAsync(key, value, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Durable persistence is a convenience layered over the applied
            // scope; never let it fail a mutation that has already taken effect.
        }
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
