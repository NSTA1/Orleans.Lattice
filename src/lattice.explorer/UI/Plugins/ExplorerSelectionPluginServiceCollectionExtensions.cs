using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The one-call composite over the Explorer's per-selection surfaces.
/// <para>
/// Each surface is an independent package that registers itself - there is no
/// shared registry of them and no per-surface flag. This type exists only
/// because a composite over six packages has to be declared somewhere that
/// references all six, and the shared UI is the one place that does. It adds no
/// coupling a head does not already have: calling the six methods directly is
/// equivalent, and is what a head that wants a subset does.
/// </para>
/// </summary>
public static class ExplorerSelectionPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Explorer's own per-selection surfaces - the metrics,
    /// topology, data and dead-letter surfaces a tree or view resolves to, the
    /// tag-index browser a tag-index selection resolves to, and the per-key
    /// revision timeline the data surface renders inline behind a row's History
    /// button.
    /// <para>
    /// Five of them occupy the <see cref="ExplorerPluginSurface.Selection"/>
    /// surface, so they are enumerated, ordered and gated by exactly the
    /// machinery that serves the area tier. The revision timeline is not one of
    /// them and never has been: it is reached from a row rather than from the
    /// strip, so it is contributed as a nested surface instead. Withholding any
    /// one call renders no tab for that surface - or, for the timeline, no
    /// History button - and adds no other coupling to remove.
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
