namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default <see cref="IExplorerPluginDomainResolver"/>: it reads each
/// registered plugin's declared contract from the catalog and resolves exactly
/// that type from the container.
/// </summary>
/// <param name="catalog">The registered plugins and their declared contracts.</param>
/// <param name="services">The container the declared contract is resolved from.</param>
public sealed class ExplorerPluginDomainResolver(
    IExplorerPluginCatalog catalog,
    IServiceProvider services) : IExplorerPluginDomainResolver
{
    private readonly IExplorerPluginCatalog _catalog =
        catalog ?? throw new ArgumentNullException(nameof(catalog));

    private readonly IServiceProvider _services =
        services ?? throw new ArgumentNullException(nameof(services));

    /// <inheritdoc />
    public Type? GetDeclaredContract(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        return _catalog.Find(pluginId)?.DomainContract;
    }

    /// <inheritdoc />
    public TDomain Resolve<TDomain>(string pluginId) where TDomain : class
    {
        ArgumentNullException.ThrowIfNull(pluginId);

        var plugin = _catalog.Find(pluginId)
            ?? throw new ExplorerPluginDomainException(
                $"No plugin is registered under id '{pluginId}', so it has no domain contract to resolve.");

        var declared = plugin.DomainContract
            ?? throw new ExplorerPluginDomainException(
                $"Plugin '{pluginId}' declares no domain contract, so '{typeof(TDomain)}' cannot be resolved for it.");

        if (declared != typeof(TDomain))
        {
            throw new ExplorerPluginDomainException(
                $"Plugin '{pluginId}' declares domain contract '{declared}' and may not resolve '{typeof(TDomain)}'.");
        }

        return _services.GetService(declared) as TDomain
            ?? throw new ExplorerPluginDomainException(
                $"Plugin '{pluginId}' declares domain contract '{declared}', but no such service is registered.");
    }

    /// <inheritdoc />
    public bool TryResolve<TDomain>(string pluginId, out TDomain? domain) where TDomain : class
    {
        ArgumentNullException.ThrowIfNull(pluginId);

        domain = null;

        var declared = _catalog.Find(pluginId)?.DomainContract;
        if (declared is null || declared != typeof(TDomain))
        {
            return false;
        }

        domain = _services.GetService(declared) as TDomain;
        return domain is not null;
    }
}
