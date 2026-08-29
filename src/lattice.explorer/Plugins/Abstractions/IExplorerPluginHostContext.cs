namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Everything one plugin is entitled to see: the ambient host facts, its own
/// preference namespace, and its own declared domain contract.
/// <para>
/// A context instance is <em>bound to a single plugin id</em>. That is what
/// makes the boundary real rather than advisory: there is no parameter through
/// which a plugin can name another plugin, so it cannot read another plugin's
/// preferences and cannot resolve another plugin's domain model. The contract
/// exposes no cluster connection, no gRPC channel, and no service locator, so
/// a plugin's reach is exactly its declared domain contract plus the facts
/// below.
/// </para>
/// </summary>
public interface IExplorerPluginHostContext
{
    /// <summary>The plugin this context is bound to. Never <see langword="null"/>.</summary>
    string PluginId { get; }

    /// <summary>
    /// The currently selected tree or view, or <see langword="null"/> when none
    /// is selected. A <see cref="ExplorerPluginSurface.Selection"/> plugin
    /// renders against this; an <see cref="ExplorerPluginSurface.Area"/> plugin
    /// may ignore it.
    /// </summary>
    ExplorerPluginSelection? Selection { get; }

    /// <summary>The current connection health.</summary>
    ExplorerPluginConnectionStatus Connection { get; }

    /// <summary>The active tenant and the host-resolved effective visibility.</summary>
    ExplorerPluginTenantScope Tenant { get; }

    /// <summary>
    /// This plugin's own preference namespace. Two plugins may use the same
    /// key without colliding, and neither can read the other's.
    /// </summary>
    IExplorerPluginPreferences Preferences { get; }

    /// <summary>
    /// Raised after one of the ambient facts changes, carrying which one so a
    /// plugin can re-render only for what it reads.
    /// </summary>
    event Action<ExplorerPluginHostChange>? Changed;

    /// <summary>
    /// Resolves this plugin's declared domain contract. This is the controlled
    /// domain-model seam: <typeparamref name="TDomain"/> must be exactly the
    /// type the plugin declared through
    /// <see cref="IExplorerPlugin.DomainContract"/>, so a plugin can only reach
    /// what its own source says it reaches.
    /// </summary>
    /// <typeparam name="TDomain">The declared domain contract type.</typeparam>
    /// <returns>The resolved domain model.</returns>
    /// <exception cref="ExplorerPluginDomainException">
    /// The plugin declared no domain contract, declared a different one, or its
    /// declared contract is not registered.
    /// </exception>
    TDomain GetDomain<TDomain>() where TDomain : class;

    /// <summary>
    /// The non-throwing form of <see cref="GetDomain{TDomain}"/>. Returns
    /// <see langword="false"/> for every non-resolution, including the case
    /// where <typeparamref name="TDomain"/> is not the declared contract, so
    /// prefer <see cref="GetDomain{TDomain}"/> when a mismatch should be a hard
    /// failure rather than a silent absence.
    /// </summary>
    /// <typeparam name="TDomain">The declared domain contract type.</typeparam>
    /// <param name="domain">The resolved domain model, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the domain model was resolved.</returns>
    bool TryGetDomain<TDomain>(out TDomain? domain) where TDomain : class;
}
