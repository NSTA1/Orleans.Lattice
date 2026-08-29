using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// Registration for the tag-index browsing per-selection plugin.
/// </summary>
public static class TagIndexPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tag-index browsing surface: the plugin itself, the domain
    /// contract the host resolves for it, and the shared per-selection kernel.
    /// Idempotent, so a head may call it alongside the composite registration.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerTagIndexPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerSelectionPluginHost();
        services.TryAddScoped<ITagIndexSurface, TagIndexSurface>();

        return services.AddExplorerPlugin<TagIndexSelectionPlugin>();
    }
}
