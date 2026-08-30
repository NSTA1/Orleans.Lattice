using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The controlled domain model the Explorer's <b>platform-operator</b> tenancy
/// surface operates against: the whole of what the Tenants plugin's view may
/// reach.
/// <para>
/// This is the wide half of the epic's D3 seam for tenancy. A tenancy panel
/// receives an <see cref="IExplorerPluginHostContext"/> bound to its own plugin
/// id and resolves exactly one domain contract from it; it never takes the
/// cluster connection, the gRPC channel, the tenant-administration wire types,
/// or another plugin's services. Widening what a tenancy plugin can reach is an
/// edit on this interface (or on <see cref="IMyTenantDomain"/>) and nowhere
/// else.
/// </para>
/// <para>
/// It extends <see cref="IMyTenantDomain"/>, the tenant-administrator contract
/// the My Tenant plugin declares, with the operator-only reach: the full
/// <see cref="ITenantAdminService"/> in place of the narrowed operations
/// surface, plus platform-operator validation. A surface handed only
/// <see cref="IMyTenantDomain"/> therefore cannot reach any of it (issue #1785).
/// </para>
/// <para>
/// It deliberately re-exposes only three things: the tenancy operations, the
/// availability probe that decides whether the surface exists at all, and the
/// Explorer's <em>existing</em> tenant-identity seam. It does not duplicate
/// tenant identity - <see cref="IMyTenantDomain.ActiveTenant"/>,
/// <see cref="IsPlatformOperatorAsync"/>, and the two mutations below all
/// delegate to <see cref="IExplorerTenantSwitcher"/>, which re-validates the
/// caller against <see cref="IExplorerTenantOperatorGate"/> on every call, so a
/// plugin can never self-elevate through this seam.
/// </para>
/// </summary>
/// <seealso cref="IExplorerPluginHostContext"/>
/// <seealso cref="IMyTenantDomain"/>
public interface ITenancyDomain : IMyTenantDomain
{
    /// <summary>
    /// The full tenancy operations surface, including the operator-only
    /// operations <see cref="IMyTenantDomain.Tenants"/> withholds. Folds a server
    /// refusal or a transport failure into a status rather than throwing, so the
    /// view stays thin. Never <see langword="null"/>.
    /// </summary>
    new ITenantAdminService Tenants { get; }

    /// <summary>
    /// Returns <see langword="true"/> when the current caller validates as a
    /// platform operator, through the Explorer's existing operator gate.
    /// Advisory: the cluster re-enforces on every call.
    /// </summary>
    /// <param name="cancellationToken">Cancels the validation.</param>
    /// <returns><see langword="true"/> for a validated platform operator.</returns>
    ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default);
}
