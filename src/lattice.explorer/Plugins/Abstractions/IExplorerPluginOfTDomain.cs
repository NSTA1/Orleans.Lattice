namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// A plugin that declares its controlled domain contract in the type system.
/// Implementing this instead of <see cref="IExplorerPlugin"/> supplies
/// <see cref="IExplorerPlugin.DomainContract"/> automatically, so the declared
/// reach of the plugin is a compile-time fact stated once in its signature.
/// </summary>
/// <typeparam name="TDomain">
/// The single domain contract the host will resolve for this plugin. The host
/// hands the plugin this type and nothing else, so it is the reviewable
/// boundary of what the plugin can reach.
/// </typeparam>
public interface IExplorerPlugin<TDomain> : IExplorerPlugin
    where TDomain : class
{
    /// <inheritdoc />
    Type? IExplorerPlugin.DomainContract => typeof(TDomain);
}
