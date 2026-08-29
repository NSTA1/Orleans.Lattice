using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// Registration for the per-key revision-timeline surface.
/// </summary>
public static class HistoryPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the revision-timeline surface: the nested-view contribution the
    /// value drill-down surface renders behind a row's History button, the domain
    /// contract that is the whole of the view's reach, and the shared
    /// per-selection kernel. Idempotent, so a head may call it alongside the
    /// composite registration.
    /// <para>
    /// This registers no tier tab, because the timeline is not one: it is reached
    /// from a row, exactly as it is today. Withholding this call simply removes
    /// the History button.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerHistorySurface(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IHistorySurface, HistorySurface>();

        return services.AddExplorerSelectionNestedSurface<EntryHistoryNestedSurface>();
    }
}
