using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// Registration helpers for the shell's banner contributions.
/// </summary>
/// <remarks>
/// <para>
/// A feature that owns a piece of shared chrome - the tenant scope control, the
/// theme and density controls - registers its component here and the shell
/// renders it. Neither side references the other: the feature names a
/// placement, the shell renders whatever is registered against one.
/// </para>
/// <para>
/// The catalog registers itself on the first contribution, so a head opts in by
/// calling this and by nothing else. A deployment that contributes nothing has
/// no catalog and the shell simply renders the regions empty.
/// </para>
/// </remarks>
public static class ExplorerChromeServiceCollectionExtensions
{
    /// <summary>
    /// Contributes <typeparamref name="TComponent"/> to
    /// <paramref name="placement"/> in the shell's banner.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <param name="placement">The banner region to render into.</param>
    /// <param name="order">
    /// The ordering hint within the placement, ascending. Contributions with
    /// equal hints keep registration order.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerChromeSlot<TComponent>(
        this IServiceCollection services,
        ExplorerChromeSlotPlacement placement,
        int order = 0)
        where TComponent : IComponent
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(new ExplorerChromeSlot(placement, typeof(TComponent), order));

        // Grouping is a statement about the application, not about a session, so
        // the catalog is a singleton. TryAdd, because every contribution calls
        // this and only the first needs to create it.
        services.TryAddSingleton<IExplorerChromeSlotCatalog>(static provider =>
            new ExplorerChromeSlotCatalog(provider.GetServices<ExplorerChromeSlot>()));

        return services;
    }
}
