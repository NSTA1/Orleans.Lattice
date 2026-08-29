using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// Registration helpers for the Explorer shell's plugin host: the two adapters
/// the plugin contract deliberately cannot supply itself, one method per
/// area plugin the shared UI ships, and the shared UI's own per-selection
/// plugin set.
/// <para>
/// A head chooses its plugin set by which of these it calls. There is no
/// per-area option flag and no shared registry to edit: withholding a plugin is
/// simply not registering it.
/// </para>
/// <para>
/// A plugin that lives in its own package registers itself from that package
/// instead - <c>AddExplorerBackupsPlugin</c> ships with the Backups plugin,
/// <c>AddExplorerAccessPlugin</c> with the Access plugin, and
/// <c>AddExplorerSchemaPlugin</c> with the Schema plugin - so this type shrinks
/// as each remaining area is converted.
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

        return services;
    }

    /// <summary>
    /// Registers the Explorer's own per-selection plugins - the metrics,
    /// topology, data and dead-letter surfaces a tree or view resolves to, and
    /// the tag-index browser a tag-index selection resolves to.
    /// <para>
    /// These occupy the <see cref="ExplorerPluginSurface.Selection"/> surface, so
    /// they are enumerated, ordered and gated by exactly the machinery that
    /// serves the area tier. A head that wants a subset registers the individual
    /// plugin types through
    /// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPlugin{TPlugin}"/>
    /// instead of calling this; withholding one renders no tab for it, and adds
    /// no other coupling to remove.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSelectionPlugins(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginAdapters();
        services.AddExplorerPlugin<MetricsSelectionPlugin>();
        services.AddExplorerPlugin<TopologySelectionPlugin>();
        services.AddExplorerPlugin<DataSelectionPlugin>();
        services.AddExplorerPlugin<DeadLetterSelectionPlugin>();
        return services.AddExplorerPlugin<TagIndexSelectionPlugin>();
    }
}
