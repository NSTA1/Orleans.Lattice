using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// Registration for the machinery every per-selection plugin shares.
/// <para>
/// Each per-selection package calls this from its own <c>AddExplorer*</c>
/// method, so a head that registers a single surface still gets a working tier
/// and a head that registers the whole set pays for the machinery once.
/// Idempotent.
/// </para>
/// </summary>
public static class SelectionPluginHostServiceCollectionExtensions
{
    /// <summary>
    /// Registers the per-selection kernel: the nested-surface registry a hosting
    /// surface resolves an inline view through, plus the plugin host itself.
    /// Scoped per Blazor circuit.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSelectionPluginHost(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginHost();
        services.TryAddScoped<ISelectionNestedSurfaceRegistry, SelectionNestedSurfaceRegistry>();

        return services;
    }

    /// <summary>
    /// Contributes <typeparamref name="TSurface"/> as a nested per-selection
    /// view, so a hosting surface can render it inline without referencing this
    /// package. Registering the same implementation type twice is a no-op.
    /// </summary>
    /// <typeparam name="TSurface">The nested-surface contribution to register.</typeparam>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSelectionNestedSurface<TSurface>(this IServiceCollection services)
        where TSurface : class, ISelectionNestedSurface
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerSelectionPluginHost();
        services.TryAddEnumerable(ServiceDescriptor.Scoped<ISelectionNestedSurface, TSurface>());

        return services;
    }
}
