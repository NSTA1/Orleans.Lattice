using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The controlled domain model a <b>tenant-administrator</b> surface operates
/// against: the tenant's own operations, the availability probe that decides
/// whether the surface exists at all, and the read side of the Explorer's
/// tenant-identity seam.
/// <para>
/// This is the narrow half of the tenancy D3 seam (issue #1785). The My Tenant
/// plugin declares exactly this contract, so what it can reach is a compile-time
/// fact readable from its own source: it cannot author quota ceilings, widen the
/// allowed region set, or run the tenant lifecycle, because none of those is on
/// the interface it is handed. The cluster refuses them too, and Blazor never
/// binds an event handler to one - but the type system now says so first, which
/// is the whole point of D3.
/// </para>
/// <para>
/// <see cref="ITenancyDomain"/> extends this contract with the
/// platform-operator surface and is what the Tenants plugin receives.
/// </para>
/// </summary>
/// <seealso cref="IExplorerPluginHostContext"/>
/// <seealso cref="ITenancyDomain"/>
public interface IMyTenantDomain
{
    /// <summary>
    /// The tenancy operations surface, narrowed to what a tenant administrator
    /// may do to its own tenant. Folds a server refusal or a transport failure
    /// into a status rather than throwing, so the view stays thin. Never
    /// <see langword="null"/>.
    /// </summary>
    ITenantSelfAdminService Tenants { get; }

    /// <summary>
    /// <see langword="true"/> when the Explorer's tenant-view seam is enabled, so
    /// a tenancy surface is meaningful at all. <see langword="false"/> for a
    /// deployment without the tenancy add-on, where every tenancy plugin should
    /// render nothing.
    /// </summary>
    bool IsTenancyEnabled { get; }

    /// <summary>
    /// The caller's active tenant, or <see langword="null"/> when none is
    /// established. This is what a self-service surface scopes itself to.
    /// </summary>
    ExplorerTenantId? ActiveTenant { get; }

    /// <summary>
    /// The visibility the caller has currently requested. An unvalidated
    /// cross-tenant request degrades, fail-closed, to the active tenant.
    /// </summary>
    ExplorerTenantVisibility RequestedVisibility { get; }

    /// <summary>
    /// Probes whether the tenancy surfaces exist for the current caller,
    /// resolving onto the shell's four-state access model. Never throws.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The resolved access decision.</returns>
    ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Switches the caller's active tenant, so the surface re-scopes to the
    /// tenant switched to. Honoured only for a validated platform operator; a
    /// non-operator request changes nothing, because it delegates to
    /// <see cref="IExplorerTenantSwitcher"/>, which re-validates the caller
    /// against <see cref="IExplorerTenantOperatorGate"/> on every call.
    /// </summary>
    /// <param name="tenant">The tenant to switch to.</param>
    /// <param name="cancellationToken">Cancels the validation.</param>
    /// <returns>
    /// <see langword="true"/> when the switch was applied;
    /// <see langword="false"/> when it was denied and nothing changed.
    /// </returns>
    ValueTask<bool> SwitchTenantAsync(ExplorerTenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>
    /// Requests a visibility scope. Honoured only for a validated platform
    /// operator; a non-operator request changes nothing, for the same reason
    /// <see cref="SwitchTenantAsync"/> does. A plugin can therefore never
    /// self-elevate through this seam.
    /// </summary>
    /// <param name="visibility">The visibility to request.</param>
    /// <param name="cancellationToken">Cancels the validation.</param>
    /// <returns>
    /// <see langword="true"/> when the request was applied;
    /// <see langword="false"/> when it was denied and nothing changed.
    /// </returns>
    ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default);
}
