namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The fail-closed view-scoping seam over the Explorer's catalog and data. By
/// default every listing is scoped to the caller's active tenant
/// (<see cref="IExplorerTenantContext.ActiveTenant"/>) so a tenant sees only its
/// own trees and data; the cross-tenant, all-tenant view is granted only when the
/// caller both requests <see cref="ExplorerTenantVisibility.AllTenants"/> and
/// validates as a platform operator. An anonymous, denied, or un-asserted caller
/// falls through, fail-closed, to the active-tenant scope, so a tenant can never
/// see another tenant's data through the Explorer.
/// </summary>
/// <remarks>
/// This is the single narrowest client-side visibility seam, mirroring the
/// cluster's server-side observability and enumeration seams. Tenant ownership is
/// derived from the physical tree id's <c>t/</c> prefix
/// (<see cref="ExplorerTenantTrees"/>), never from a wire-supplied classification,
/// and the operator is validated through <see cref="IExplorerTenantOperatorGate"/>,
/// never trusted. When tenant scoping is not enabled the registered view is
/// inactive (<see cref="IsActive"/> is <see langword="false"/>) and callers skip
/// scoping entirely, so a non-tenant cluster's Explorer is byte-for-byte
/// unchanged.
/// </remarks>
public interface IExplorerTenantView
{
    /// <summary>
    /// <see langword="true"/> when tenant scoping is enabled and callers must scope
    /// their listings through this view. <see langword="false"/> for the inactive
    /// view, which every caller treats as "no scoping" so the off path is unchanged.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// The caller's active tenant, or <see langword="null"/> when none is
    /// established. Surfaced for the shell (for example to label the active-tenant
    /// view); scoping decisions go through <see cref="ScopeAsync{TItem}"/> and
    /// <see cref="IsVisible"/>.
    /// </summary>
    ExplorerTenantId? ActiveTenant { get; }

    /// <summary>
    /// Resolves the effective visibility for the current caller. Returns
    /// <see cref="ExplorerTenantVisibility.AllTenants"/> only when the caller
    /// requested it <em>and</em> validates as a platform operator; otherwise
    /// returns <see cref="ExplorerTenantVisibility.ActiveTenant"/> (the fail-closed
    /// default).
    /// </summary>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns>The effective visibility the view will enforce.</returns>
    ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <see langword="true"/> when a tree with id <paramref name="treeId"/>
    /// is visible under <paramref name="effectiveVisibility"/>. Under
    /// <see cref="ExplorerTenantVisibility.AllTenants"/> every tree is visible;
    /// under <see cref="ExplorerTenantVisibility.ActiveTenant"/> only a tree owned
    /// by the caller's active tenant is visible, and nothing is visible when no
    /// active tenant is established.
    /// </summary>
    /// <param name="effectiveVisibility">The already-resolved effective visibility.</param>
    /// <param name="treeId">The physical tree id to test. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the tree is visible.</returns>
    bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId);

    /// <summary>
    /// Scopes <paramref name="items"/> to the caller's effective visibility. Under
    /// the all-tenant view the list is returned unchanged (same reference); under
    /// the active-tenant view only items owned by the caller's active tenant are
    /// kept, and an empty list is returned when no active tenant is established.
    /// </summary>
    /// <typeparam name="TItem">The catalog item type being scoped.</typeparam>
    /// <param name="items">The items to scope. Must not be <see langword="null"/>.</param>
    /// <param name="treeIdSelector">
    /// Extracts the physical tree id from an item for ownership classification.
    /// Must not be <see langword="null"/>. Pass a cached <c>static</c> lambda to
    /// avoid a per-call closure allocation on the listing hot path.
    /// </param>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns>The scoped items.</returns>
    ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
        IReadOnlyList<TItem> items,
        Func<TItem, string> treeIdSelector,
        CancellationToken cancellationToken = default);
}
