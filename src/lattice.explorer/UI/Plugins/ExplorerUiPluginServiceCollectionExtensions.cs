using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// Registration helpers for the Explorer shell's plugin host: the two adapters
/// the plugin contract deliberately cannot supply itself, and one method per
/// area plugin the shared UI ships.
/// <para>
/// A head chooses its area set by which of these it calls. There is no per-area
/// option flag and no shared registry to edit: withholding an area is simply
/// not registering its plugin.
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
    /// Registers the Backups area plugin. The Backups feature itself must be
    /// registered separately (it owns the control client and the access gate).
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerBackupsPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerPluginAdapters();
        return services.AddExplorerPlugin<BackupsAreaPlugin>();
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
}
