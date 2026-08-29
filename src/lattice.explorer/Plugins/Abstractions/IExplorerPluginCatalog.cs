namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The registered plugin set, resolved once and presented in a stable display
/// order.
/// <para>
/// Ordering is a total order and does not depend on container registration
/// order: plugins sort by
/// <see cref="ExplorerPluginDescriptor.Order"/>, then by
/// <see cref="ExplorerPluginDescriptor.Label"/>, then by
/// <see cref="ExplorerPluginDescriptor.PluginId"/>, all ordinal. Two plugins
/// that share an ordering hint therefore still render in the same sequence on
/// every head and every run.
/// </para>
/// </summary>
public interface IExplorerPluginCatalog
{
    /// <summary>Every registered plugin, in display order.</summary>
    IReadOnlyList<IExplorerPlugin> All { get; }

    /// <summary>
    /// The registered plugins occupying <paramref name="surface"/>, in display
    /// order. Returns an empty list for a surface no plugin occupies.
    /// </summary>
    /// <param name="surface">The navigation tier to filter to.</param>
    IReadOnlyList<IExplorerPlugin> ForSurface(ExplorerPluginSurface surface);

    /// <summary>
    /// The plugin registered under <paramref name="pluginId"/>, or
    /// <see langword="null"/> when none is. Ids compare ordinally.
    /// </summary>
    /// <param name="pluginId">The plugin id to find. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    IExplorerPlugin? Find(string pluginId);
}
