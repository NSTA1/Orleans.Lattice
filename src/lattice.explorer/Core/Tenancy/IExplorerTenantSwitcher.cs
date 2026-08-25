namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The operator-gated control seam behind the Explorer's tenant selector and the
/// "all tenants" toggle. It mutates the per-circuit
/// <see cref="IExplorerTenantContext"/> - the requested visibility and the active
/// tenant - but only for a caller the
/// <see cref="IExplorerTenantOperatorGate"/> validates as a platform operator, so a
/// non-operator can never self-elevate to another tenant or the cross-tenant view.
/// </summary>
/// <remarks>
/// This is a fail-closed presentation seam: every mutation is denied unless the
/// caller validates as an operator, and it is inert entirely when the tenant view
/// is inactive (<see cref="IsActive"/> is <see langword="false"/>), so no selector
/// renders for a non-tenant deployment. It never widens what a caller can see - the
/// <see cref="IExplorerTenantView"/> re-validates the operator on every read and the
/// cluster re-enforces on every call - so it is a UX affordance layered over the
/// existing fail-closed enforcement, not a new authorization decision.
/// </remarks>
public interface IExplorerTenantSwitcher
{
    /// <summary>
    /// <see langword="true"/> when tenant scoping is enabled and the tenant
    /// selector should render; <see langword="false"/> for the inactive view, when
    /// nothing tenant-related renders and every mutation below is a no-op.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// The caller's current active tenant, or <see langword="null"/> when none is
    /// established. Mirrors <see cref="IExplorerTenantView.ActiveTenant"/>.
    /// </summary>
    ExplorerTenantId? ActiveTenant { get; }

    /// <summary>
    /// The visibility the caller has currently requested. Mirrors
    /// <see cref="IExplorerTenantContext.RequestedVisibility"/>; an operator raises
    /// it to <see cref="ExplorerTenantVisibility.AllTenants"/> through
    /// <see cref="SetVisibilityAsync"/>.
    /// </summary>
    ExplorerTenantVisibility RequestedVisibility { get; }

    /// <summary>
    /// Returns <see langword="true"/> when the current caller validates as a
    /// platform operator entitled to switch tenant and to enable the all-tenant
    /// view, and <see langword="false"/> otherwise (including for an anonymous
    /// caller or when the view is inactive). Used by the shell to decide whether to
    /// render the selector.
    /// </summary>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns><see langword="true"/> when the caller is a validated platform operator.</returns>
    ValueTask<bool> IsOperatorAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests <paramref name="visibility"/> as the caller's scope. Honoured only
    /// for a validated platform operator; a non-operator request is denied and the
    /// requested visibility is left unchanged (fail-closed). A no-op when the view
    /// is inactive.
    /// </summary>
    /// <param name="visibility">The visibility to request.</param>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns>
    /// <see langword="true"/> when the request was applied (the caller is a
    /// validated operator and the view is active); <see langword="false"/> when it
    /// was denied and nothing changed.
    /// </returns>
    ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Switches the caller's active tenant to <paramref name="tenant"/>. Honoured
    /// only for a validated platform operator; a non-operator request is denied and
    /// the active tenant is left unchanged (fail-closed). A no-op when the view is
    /// inactive.
    /// </summary>
    /// <param name="tenant">The tenant to switch to.</param>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns>
    /// <see langword="true"/> when the switch was applied (the caller is a validated
    /// operator and the view is active); <see langword="false"/> when it was denied
    /// and nothing changed.
    /// </returns>
    ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default);
}
