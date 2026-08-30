using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The head's <see cref="IExplorerTenantScopeRefresher"/>: it re-projects the
/// tenant scope onto <see cref="ExplorerPluginHostState"/> and then re-probes
/// every registered plugin gate.
/// <para>
/// That is exactly the pair of steps the shell already performs on its other
/// refresh occasions - mount, a sign-in change, and a reconnect - so wiring it
/// behind the tenancy seam makes a tenant switch the fourth of them without the
/// switcher, or the tenancy core, knowing that plugins exist.
/// </para>
/// </summary>
/// <remarks>
/// Both steps are fault-isolated internally: the host state degrades a failed
/// visibility resolution to the fail-closed active-tenant scope, and the
/// refresher contains each gate's fault so one plugin's probe can never disturb
/// another's decision. Ordering matters and is not incidental - the scope is
/// published <em>before</em> the gates are probed, so a gate that reads the
/// projected scope decides against the tenant the caller just switched to
/// rather than the one they left.
/// <para>
/// The gate refresher is supplied as a <em>deferred</em> accessor rather than
/// injected directly, and that is load-bearing. Its own graph reaches every
/// registered plugin, and a plugin may legitimately depend on the tenant
/// switcher this type is notified by, so resolving it during construction would
/// close a container cycle. Resolving it when a refresh actually runs breaks the
/// cycle without changing what is refreshed or in which order.
/// </para>
/// </remarks>
/// <param name="hostState">The adapter that publishes the projected tenant scope.</param>
/// <param name="gates">
/// Resolves the fault-isolated refresher that re-probes every plugin gate, on
/// demand rather than at construction.
/// </param>
public sealed class ExplorerPluginTenantScopeRefresher(
    ExplorerPluginHostState hostState,
    Func<IExplorerPluginAccessRefresher> gates) : IExplorerTenantScopeRefresher
{
    private readonly ExplorerPluginHostState _hostState =
        hostState ?? throw new ArgumentNullException(nameof(hostState));

    private readonly Func<IExplorerPluginAccessRefresher> _gates =
        gates ?? throw new ArgumentNullException(nameof(gates));

    /// <inheritdoc />
    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        await _hostState.RefreshTenantScopeAsync(cancellationToken).ConfigureAwait(false);
        await _gates().RefreshAsync(cancellationToken).ConfigureAwait(false);
    }
}
