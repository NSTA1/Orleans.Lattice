namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The host side of the controlled domain-model seam: it knows which domain
/// contract each registered plugin declared, and hands a plugin that one type
/// and no other.
/// <para>
/// Resolution is deliberately not a service locator. A plugin cannot ask for an
/// arbitrary service; it can ask only for the contract its own descriptor
/// declares, which makes the blast radius of a plugin readable from its
/// signature and reviewable in isolation.
/// </para>
/// </summary>
public interface IExplorerPluginDomainResolver
{
    /// <summary>
    /// The domain contract <paramref name="pluginId"/> declared, or
    /// <see langword="null"/> when it declared none or no plugin is registered
    /// under that id.
    /// </summary>
    /// <param name="pluginId">The plugin id to look up. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    Type? GetDeclaredContract(string pluginId);

    /// <summary>
    /// Resolves the domain contract declared by <paramref name="pluginId"/>.
    /// </summary>
    /// <typeparam name="TDomain">
    /// The contract to resolve. Must be exactly the declared type; a mismatch
    /// is an over-reach and fails.
    /// </typeparam>
    /// <param name="pluginId">The plugin id to resolve for. Must not be <see langword="null"/>.</param>
    /// <returns>The resolved domain model.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ExplorerPluginDomainException">
    /// No plugin is registered under <paramref name="pluginId"/>, it declared no
    /// contract, <typeparamref name="TDomain"/> is not the declared contract, or
    /// the declared contract is not registered in the container.
    /// </exception>
    TDomain Resolve<TDomain>(string pluginId) where TDomain : class;

    /// <summary>
    /// The non-throwing form of <see cref="Resolve{TDomain}"/>. Returns
    /// <see langword="false"/> for every non-resolution, including an
    /// over-reach, so prefer <see cref="Resolve{TDomain}"/> when a mismatch
    /// should be a hard failure.
    /// </summary>
    /// <typeparam name="TDomain">The contract to resolve.</typeparam>
    /// <param name="pluginId">The plugin id to resolve for. Must not be <see langword="null"/>.</param>
    /// <param name="domain">The resolved domain model, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the domain model was resolved.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    bool TryResolve<TDomain>(string pluginId, out TDomain? domain) where TDomain : class;
}
