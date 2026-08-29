using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Builds the small object graph the plugin contract tests need - a catalog, a
/// store, a domain resolver, a host-context factory and a refresher - over a
/// caller-supplied plugin set, with no container ceremony at the call site.
/// </summary>
internal sealed class PluginTestHost
{
    private PluginTestHost(
        IExplorerPluginCatalog catalog,
        ExplorerPluginAccessStore store,
        ExplorerPluginDomainResolver domains,
        ExplorerPluginHostContextFactory contexts,
        FakeExplorerPluginHostState state,
        FakeExplorerPluginPreferences preferences,
        ExplorerPluginAccessRefresher refresher)
    {
        Catalog = catalog;
        Store = store;
        Domains = domains;
        Contexts = contexts;
        State = state;
        Preferences = preferences;
        Refresher = refresher;
    }

    public IExplorerPluginCatalog Catalog { get; }

    public ExplorerPluginAccessStore Store { get; }

    public ExplorerPluginDomainResolver Domains { get; }

    public ExplorerPluginHostContextFactory Contexts { get; }

    public FakeExplorerPluginHostState State { get; }

    public FakeExplorerPluginPreferences Preferences { get; }

    public ExplorerPluginAccessRefresher Refresher { get; }

    /// <summary>
    /// Builds a host over <paramref name="plugins"/>, optionally with
    /// <paramref name="domainServices"/> registered as resolvable domain
    /// contracts.
    /// </summary>
    public static PluginTestHost Create(
        IEnumerable<IExplorerPlugin> plugins,
        Action<IServiceCollection>? domainServices = null)
    {
        var services = new ServiceCollection();
        domainServices?.Invoke(services);

        var catalog = new ExplorerPluginCatalog(plugins);
        var store = new ExplorerPluginAccessStore();
        var state = new FakeExplorerPluginHostState();
        var preferences = new FakeExplorerPluginPreferences();
        var domains = new ExplorerPluginDomainResolver(catalog, services.BuildServiceProvider());
        var contexts = new ExplorerPluginHostContextFactory(state, preferences, domains);
        var refresher = new ExplorerPluginAccessRefresher(catalog, store, contexts);

        return new PluginTestHost(catalog, store, domains, contexts, state, preferences, refresher);
    }

    /// <summary>Builds a host over <paramref name="plugins"/>.</summary>
    public static PluginTestHost Create(params IExplorerPlugin[] plugins) => Create((IEnumerable<IExplorerPlugin>)plugins);
}
