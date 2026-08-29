using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Registration helpers for the Explorer plugin model.
/// <para>
/// A head chooses its plugin set by which packages it registers: one
/// <see cref="AddExplorerPlugin{TPlugin}"/> call per plugin, and no per-area
/// option flag, no shared enum, and no edit to any shared type. Plugins are
/// compile-time registrations rather than runtime-discovered assemblies, so the
/// Explorer - an administrative surface - gains no new trust boundary.
/// </para>
/// </summary>
public static class ExplorerPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the host-side plugin machinery: the catalog, the keyed access
    /// store, the fault-isolated access refresher, the controlled domain-model
    /// resolver, and the per-plugin host-context factory. Everything is scoped
    /// per Blazor circuit, so one operator's probed access never surfaces in
    /// another's. Idempotent, and called for you by
    /// <see cref="AddExplorerPlugin{TPlugin}"/>.
    /// <para>
    /// The head must additionally register an
    /// <see cref="IExplorerPluginHostState"/> and an
    /// <see cref="IExplorerPluginPreferences"/>: those adapt the Explorer's own
    /// selection, connection, tenancy, and preference services onto the plugin
    /// contract, and live on the shell's side of the seam so this package
    /// carries no cluster dependency.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerPluginHost(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IExplorerPluginCatalog, ExplorerPluginCatalog>();
        services.TryAddScoped<IExplorerPluginAccessStore, ExplorerPluginAccessStore>();
        services.TryAddScoped<IExplorerPluginAccessRefresher, ExplorerPluginAccessRefresher>();
        services.TryAddScoped<IExplorerPluginDomainResolver, ExplorerPluginDomainResolver>();
        services.TryAddScoped<IExplorerPluginHostContextFactory, ExplorerPluginHostContextFactory>();

        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="TPlugin"/> as an Explorer plugin, along
    /// with the host machinery it needs. Registering the same implementation
    /// type twice is a no-op, so a package may safely register its own plugin
    /// from more than one composition helper.
    /// </summary>
    /// <typeparam name="TPlugin">The plugin implementation to register.</typeparam>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerPlugin<TPlugin>(this IServiceCollection services)
        where TPlugin : class, IExplorerPlugin
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerPluginHost();
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IExplorerPlugin, TPlugin>());

        return services;
    }

    /// <summary>
    /// Registers an already-constructed <paramref name="plugin"/>, for a head
    /// that composes a plugin by hand rather than through the container.
    /// Registering the same instance twice is a no-op.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <param name="plugin">The plugin instance to register. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerPlugin(this IServiceCollection services, IExplorerPlugin plugin)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(plugin);

        services.AddExplorerPluginHost();
        services.TryAddEnumerable(ServiceDescriptor.Singleton(plugin));

        return services;
    }
}
