using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// Registration helpers for the Explorer shell's plugin host: the adapters the
/// plugin contract deliberately cannot supply itself.
/// <para>
/// A head chooses its plugin set by which registrations it calls. There is no
/// per-area option flag and no shared registry to edit: withholding a plugin is
/// simply not registering it.
/// </para>
/// <para>
/// A plugin that lives in its own package registers itself from that package
/// instead - <c>AddExplorerBackupsPlugin</c> ships with the Backups plugin,
/// <c>AddExplorerAccessPlugin</c> with the Access plugin,
/// <c>AddExplorerSchemaPlugin</c> with the Schema plugin, and each per-selection
/// surface with its own - so this type now carries only what is genuinely
/// shell-side.
/// </para>
/// </summary>
public static class ExplorerUiPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the plugin host machinery plus the head-supplied adapters the
    /// contract cannot implement itself: the ambient host state (over the
    /// Explorer's selection, connection and tenant view) and the plugin
    /// preference store (over the Explorer's durable UI preference store).
    /// Scoped per Blazor circuit, so one operator's projected state never
    /// surfaces in another's. Idempotent, and called for you by each
    /// <c>AddExplorer*Plugin</c> method.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerPluginAdapters(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginHost();

        // Registered concretely as well as behind the contract: the adapter owns
        // the deterministic tenant-scope refresh the shell drives alongside the
        // gate probes, which is host machinery rather than something a plugin may
        // reach.
        services.TryAddScoped<ExplorerPluginHostState>();
        services.TryAddScoped<IExplorerPluginHostState>(
            static provider => provider.GetRequiredService<ExplorerPluginHostState>());
        services.TryAddScoped<IExplorerPluginPreferences, ExplorerPluginPreferences>();

        // Makes a tenant switch a refresh occasion alongside mount, sign-in
        // change, and reconnect. Registered here rather than by the tenancy core
        // because only the head knows there are plugin gates to re-probe; the
        // switcher resolves it optionally, so a deployment without the plugin
        // adapters simply has nothing to notify.
        //
        // The gate refresher is handed over as a deferred accessor: its graph
        // reaches every registered plugin, and a plugin may depend on the tenant
        // switcher this notifies, so resolving it eagerly would close a
        // container cycle.
        services.TryAddScoped<IExplorerTenantScopeRefresher>(
            static provider => new ExplorerPluginTenantScopeRefresher(
                provider.GetRequiredService<ExplorerPluginHostState>(),
                provider.GetRequiredService<IExplorerPluginAccessRefresher>));

        return services;
    }
}
