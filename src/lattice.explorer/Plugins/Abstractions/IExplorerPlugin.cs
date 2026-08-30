namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// One Explorer plugin: a descriptor plus three seams - the view the host
/// renders, the access gate that decides whether it is reachable, and the
/// domain contract it operates against.
/// <para>
/// A plugin is a compile-time dependency-injection registration, not a
/// runtime-discovered assembly: a head chooses its plugin set by which packages
/// it registers through
/// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPlugin{TPlugin}"/>.
/// The host enumerates <see cref="IExplorerPlugin"/> from the container and
/// needs no per-plugin knowledge.
/// </para>
/// </summary>
public interface IExplorerPlugin
{
    /// <summary>The plugin's identity and placement. Never <see langword="null"/>.</summary>
    ExplorerPluginDescriptor Descriptor { get; }

    /// <summary>
    /// The component type the host renders for this plugin. Typed as
    /// <see cref="Type"/> so the contract carries no UI-framework dependency;
    /// the shell renders it dynamically. Never <see langword="null"/>.
    /// </summary>
    Type ViewType { get; }

    /// <summary>
    /// The single domain contract this plugin operates against, or
    /// <see langword="null"/> when it needs nothing beyond
    /// <see cref="IExplorerPluginHostContext"/>. This is the controlled
    /// domain-model seam: the host resolves only this declared type for this
    /// plugin, so the plugin's reach is explicit in its own source and
    /// reviewable in isolation. Implement
    /// <see cref="IExplorerPlugin{TDomain}"/> to declare it in the type
    /// system rather than by hand.
    /// </summary>
    Type? DomainContract { get; }

    /// <summary>
    /// The plugin's own access gate. Gates are probed independently and are
    /// individually fault-isolated, so the host never carries per-plugin gating
    /// knowledge. Never <see langword="null"/>.
    /// </summary>
    IExplorerPluginAccessGate AccessGate { get; }
}
