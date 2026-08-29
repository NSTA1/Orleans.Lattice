using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;

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
/// instead - <c>AddExplorerBackupsPlugin</c> ships with the Backups plugin and
/// <c>AddExplorerAccessPlugin</c> with the Access plugin - so this type shrinks
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

        // The per-selection kernel, so the detail tier's nested-surface registry
        // resolves even on a head that registers no selection plugin at all.
        services.AddExplorerSelectionPluginHost();

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
    /// Registers the Schema area plugin. The Schema feature itself must be
    /// registered separately (it owns the control client and the access gate).
    /// A head that does not call this ships no Schema tab at all, which is what
    /// the retired per-area navigation flag was emulating.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSchemaPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerPluginAdapters();
        return services.AddExplorerPlugin<SchemaAreaPlugin>();
    }

    /// <summary>
    /// Registers the Explorer's own per-selection surfaces - the metrics,
    /// topology, data and dead-letter surfaces a tree or view resolves to, the
    /// tag-index browser a tag-index selection resolves to, and the per-key
    /// revision timeline the data surface renders inline behind a row's History
    /// button.
    /// <para>
    /// Each is an independent package with its own view, its own scoped
    /// stylesheet and its own controlled domain contract, and each ships its own
    /// <c>AddExplorer*</c> method. This is the one-call composite for a head that
    /// wants the whole tier; a head that wants a subset calls the individual
    /// methods instead, and withholding one renders no tab for it (or, for the
    /// timeline, no History button) and adds no other coupling to remove.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSelectionPlugins(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginAdapters();
        services.AddExplorerMetricsPlugin();
        services.AddExplorerTopologyPlugin();
        services.AddExplorerDataPlugin();
        services.AddExplorerHistorySurface();
        services.AddExplorerDeadLetterPlugin();
        return services.AddExplorerTagIndexPlugin();
    }
}
