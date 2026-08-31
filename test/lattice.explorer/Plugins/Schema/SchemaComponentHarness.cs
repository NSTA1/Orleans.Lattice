using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Session;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Tests.Navigation;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Plugins;

// The harness owns a ComponentTestRenderer, whose framework-internal base type
// carries this advisory; see ComponentTestRenderer for why that is worth it.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The object graph a Schema component test renders against: the keyed access
/// store the area's gating reads, a scriptable domain model registered as the
/// plugin's declared contract, and the host-context factory the panel resolves
/// that contract through.
/// <para>
/// Everything answers synchronously, so a test drives a transition and reads the
/// result with no delay, no polling, and no dependence on timing or ordering.
/// </para>
/// </summary>
internal sealed class SchemaComponentHarness : IDisposable
{
    private readonly ServiceProvider _provider;

    private SchemaComponentHarness(
        ServiceProvider provider,
        ComponentTestRenderer renderer,
        ExplorerPluginAccessStore store,
        FakeSchemaPluginDomain domain)
    {
        _provider = provider;
        Renderer = renderer;
        Store = store;
        Domain = domain;
    }

    public ComponentTestRenderer Renderer { get; }

    public ExplorerPluginAccessStore Store { get; }

    public FakeSchemaPluginDomain Domain { get; }

    public static SchemaComponentHarness Create()
    {
        var domain = new FakeSchemaPluginDomain();
        var store = new ExplorerPluginAccessStore();
        var plugin = new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>());
        var catalog = new ExplorerPluginCatalog([plugin]);

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);
        var selection = Substitute.For<IExplorerSelection>();
        selection.Selected.Returns((CatalogItem?)null);
        var hostState = new ExplorerPluginHostState(selection, connection);

        var services = new ServiceCollection();
        services.AddLogging();

        // The shell-state contract the panel remembers and addresses its open
        // surface on. Registered as the real thing: the route model is a pure
        // in-memory type and the preference store falls back to an in-memory
        // backing store, so nothing here reaches a browser.
        services.AddExplorerSession();
        services.AddSingleton<ISchemaPluginDomain>(domain);
        services.AddSingleton<IExplorerPluginCatalog>(catalog);
        services.AddSingleton<IExplorerPluginAccessStore>(store);
        services.AddSingleton<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
        services.AddSingleton<IExplorerPluginDomainResolver>(
            provider => new ExplorerPluginDomainResolver(catalog, provider));
        services.AddSingleton<IExplorerPluginHostContextFactory>(
            provider => new ExplorerPluginHostContextFactory(
                hostState,
                provider.GetRequiredService<IExplorerPluginPreferences>(),
                provider.GetRequiredService<IExplorerPluginDomainResolver>()));

        var provider = services.BuildServiceProvider();
        var renderer = new ComponentTestRenderer(provider, provider.GetRequiredService<ILoggerFactory>());

        return new SchemaComponentHarness(provider, renderer, store, domain);
    }

    /// <summary>Files the plugin-level decision the area's coarse gate reads.</summary>
    public void Allow() => Store.Set(SchemaPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

    /// <summary>Renders <typeparamref name="TComponent"/> as a root and returns its component id.</summary>
    public async Task<int> RenderAsync<TComponent>(params (string Name, object? Value)[] parameters)
        where TComponent : IComponent
    {
        var (id, _) = await RenderWithInstanceAsync<TComponent>(parameters);
        return id;
    }

    /// <summary>
    /// Renders <typeparamref name="TComponent"/> as a root and returns both its
    /// component id and the instance, so a test can drive a public method on the
    /// component rather than reaching for the markup a child component owns.
    /// </summary>
    /// <typeparam name="TComponent">The component to render.</typeparam>
    /// <param name="parameters">The parameters to render it with.</param>
    public Task<(int Id, TComponent Component)> RenderWithInstanceAsync<TComponent>(
        params (string Name, object? Value)[] parameters)
        where TComponent : IComponent
    {
        var bag = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (name, value) in parameters)
        {
            bag[name] = value;
        }

        return Renderer.RenderAsync<TComponent>(ParameterView.FromDictionary(bag));
    }

    /// <summary>Builds a session over the harness's domain, optionally probed for a tree.</summary>
    public async Task<SchemaSession> SessionAsync(string? treeId, bool allowed = true)
    {
        var session = new SchemaSession(Domain) { IsAllowed = allowed };
        if (treeId is not null)
        {
            session.TreeId = treeId;
            session.Grants = await Domain.ProbeTreeAsync(treeId);
        }

        return session;
    }

    public void Dispose()
    {
        Renderer.Dispose();
        _provider.Dispose();
    }
}
