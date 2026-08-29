using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema;
using Orleans.Lattice.Explorer.Plugins.Schema.Components;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema area as a self-contained plugin: its descriptor, its view, its own
/// gate, the single controlled domain contract it declares, and the one
/// registration call that is now the whole of a head's opt-in.
/// </summary>
[TestFixture]
public sealed class SchemaAreaPluginTests
{
    [Test]
    public void The_plugin_declares_a_stable_dotted_id_on_the_area_surface()
    {
        var plugin = new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>());

        Assert.Multiple(() =>
        {
            Assert.That(plugin.Descriptor.PluginId, Is.EqualTo(SchemaPluginKeys.PluginId));
            Assert.That(plugin.Descriptor.PluginId, Does.StartWith("orleans.lattice."));
            Assert.That(plugin.Descriptor.Label, Is.EqualTo("Schema"));
            Assert.That(plugin.Descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
        });
    }

    [Test]
    public void The_plugin_renders_its_own_panel_and_gates_on_its_own_feature_service()
    {
        var gate = Substitute.For<ISchemaAdminCapabilityService>();

        var plugin = new SchemaAreaPlugin(gate);

        Assert.Multiple(() =>
        {
            Assert.That(plugin.ViewType, Is.EqualTo(typeof(SchemaPanel)));
            Assert.That(plugin.AccessGate, Is.SameAs(gate));
        });
    }

    [Test]
    public void The_plugin_declares_the_schema_domain_as_its_only_contract()
    {
        var plugin = new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>());

        Assert.That(
            ((IExplorerPlugin)plugin).DomainContract,
            Is.EqualTo(typeof(ISchemaPluginDomain)),
            "the declared contract is the whole of the plugin's reach");
    }

    [Test]
    public void The_plugin_rejects_a_null_gate()
    {
        Assert.That(() => new SchemaAreaPlugin(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_schema_plugin_sorts_after_the_ui_shipped_areas()
    {
        var catalog = new ExplorerPluginCatalog(new IExplorerPlugin[]
        {
            new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>()),
            new AccessAreaPlugin(Substitute.For<IAuthAdminCapabilityService>()),
            new BackupsAreaPlugin(Substitute.For<IBackupCapabilityService>()),
        });

        Assert.That(
            catalog.ForSurface(ExplorerPluginSurface.Area).Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[]
            {
                BackupsPluginKeys.PluginId,
                AccessPluginKeys.PluginId,
                SchemaPluginKeys.PluginId,
            }));
    }

    [Test]
    public void AddExplorerSchemaPlugin_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddExplorerSchemaPlugin(),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task AddExplorerSchemaPlugin_registers_the_plugin_its_domain_and_the_feature_services()
    {
        await using var provider = BuildHost(services => services.AddExplorerSchemaPlugin());
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                AreaPluginIds(scope.ServiceProvider),
                Is.EqualTo(new[] { SchemaPluginKeys.PluginId }));
            Assert.That(
                scope.ServiceProvider.GetService<ISchemaPluginDomain>(),
                Is.TypeOf<SchemaPluginDomain>(),
                "the one registration must wire the controlled domain model the panel resolves");
            Assert.That(
                scope.ServiceProvider.GetService<ISchemaAdminCapabilityService>(),
                Is.Not.Null,
                "the plugin owns the feature services its domain composes");
        });
    }

    [Test]
    public async Task Registering_the_schema_plugin_twice_is_a_no_op()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerSchemaPlugin();
            services.AddExplorerSchemaPlugin();
        });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(),
            Is.EqualTo(1),
            "a duplicate registration must not fail the catalog's unique-id check");
    }

    [Test]
    public async Task Not_registering_the_schema_plugin_surfaces_no_area()
    {
        await using var provider = BuildHost(_ => { });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerPlugin>(),
            Is.Empty,
            "withholding the area is simply not registering its plugin");
    }

    [Test]
    public async Task The_registered_plugin_resolves_its_declared_domain_through_the_host_context()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerPluginAdapters();
            services.AddExplorerSchemaPlugin();
        });
        await using var scope = provider.CreateAsyncScope();

        var context = scope.ServiceProvider
            .GetRequiredService<IExplorerPluginHostContextFactory>()
            .Create(SchemaPluginKeys.PluginId);

        Assert.That(context.GetDomain<ISchemaPluginDomain>(), Is.Not.Null);
    }

    private static string[] AreaPluginIds(IServiceProvider provider) => provider
        .GetRequiredService<IExplorerPluginCatalog>()
        .ForSurface(ExplorerPluginSurface.Area)
        .Select(plugin => plugin.Descriptor.PluginId)
        .ToArray();

    private static ServiceProvider BuildHost(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        configure(services);
        return services.BuildServiceProvider();
    }
}
