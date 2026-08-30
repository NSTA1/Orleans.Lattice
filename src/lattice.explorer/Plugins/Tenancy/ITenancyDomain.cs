using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The controlled domain model the Explorer's tenancy plugins operate against:
/// the single contract the host resolves for a tenancy plugin, and the whole of
/// what its view may reach.
/// <para>
/// This is the epic's D3 seam for tenancy. A tenancy panel receives an
/// <see cref="IExplorerPluginHostContext"/> bound to its own plugin id and
/// resolves exactly this type from it; it never takes the cluster connection,
/// the gRPC channel, the tenant-administration wire types, or another plugin's
/// services. Widening what a tenancy plugin can reach is an edit on this
/// interface and nowhere else.
/// </para>
/// <para>
/// It deliberately re-exposes only three things: the tenancy operations, the
/// availability probe that decides whether the surface exists at all, and the
/// Explorer's <em>existing</em> tenant-identity seam. It does not duplicate
/// tenant identity - <see cref="ActiveTenant"/>,
/// <see cref="IsPlatformOperatorAsync"/>, and the two mutations below all
/// delegate to <see cref="IExplorerTenantSwitcher"/>, which re-validates the
/// caller against <see cref="IExplorerTenantOperatorGate"/> on every call, so a
/// plugin can never self-elevate through this seam.
/// </para>
/// </summary>
/// <seealso cref="IExplorerPluginHostContext"/>
public interface ITenancyDomain
{
    /// <summary>
    /// The tenancy operations surface. Folds a server refusal or a transport
    /// failure into a status rather than throwing, so the view stays thin. Never
    /// <see langword="null"/>.
    /// </summary>
    ITenantAdminService Tenants { get; }

    /// <summary>
    /// <see langword="true"/> when the Explorer's tenant-view seam is enabled, so
    /// a tenancy surface is meaningful at all. <see langword="false"/> for a
    /// deployment without the tenancy add-on, where every tenancy plugin should
    /// render nothing.
    /// </summary>
    bool IsTenancyEnabled { get; }

    /// <summary>
    /// The caller's active tenant, or <see langword="null"/> when none is
    /// established. This is what a self-service surface scopes itself to, and
    /// what an operator changes by switching tenant.
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
    /// Returns <see langword="true"/> when the current caller validates as a
    /// platform operator, through the Explorer's existing operator gate.
    /// Advisory: the cluster re-enforces on every call.
    /// </summary>
    /// <param name="cancellationToken">Cancels the validation.</param>
    /// <returns><see langword="true"/> for a validated platform operator.</returns>
    ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Switches the caller's active tenant. Honoured only for a validated
    /// platform operator; a non-operator request changes nothing.
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
    /// operator; a non-operator request changes nothing.
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
