namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Creates the per-plugin <see cref="IExplorerPluginHostContext"/> the host
/// hands to a plugin and to that plugin's access gate.
/// <para>
/// The factory is the single place the host decides what a plugin may see, so
/// it is the seam to read when reviewing a plugin's reach. Contexts are bound
/// to a plugin id and are stable for the lifetime of the factory, so a plugin
/// that subscribes to
/// <see cref="IExplorerPluginHostContext.Changed"/> keeps a single
/// subscription rather than one per render.
/// </para>
/// </summary>
public interface IExplorerPluginHostContextFactory
{
    /// <summary>
    /// Returns the context bound to <paramref name="pluginId"/>. Repeated calls
    /// for the same id return the same instance.
    /// </summary>
    /// <param name="pluginId">The plugin to bind the context to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    IExplorerPluginHostContext Create(string pluginId);
}
