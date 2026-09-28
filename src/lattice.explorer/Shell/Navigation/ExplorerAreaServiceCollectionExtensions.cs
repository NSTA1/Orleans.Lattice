using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// How a native area registers itself: the only way to add an
/// <see cref="IExplorerArea"/>, and <see langword="internal"/> so no assembly
/// but the Shell can (epic decision E2).
/// </summary>
internal static class ExplorerAreaServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="TArea"/> as a native area, resolved per
    /// circuit. Registering the same area twice registers it once.
    /// </summary>
    /// <typeparam name="TArea">The area.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The same service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerArea<TArea>(this IServiceCollection services)
        where TArea : class, IExplorerArea
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IExplorerArea, TArea>());
        return services;
    }
}
