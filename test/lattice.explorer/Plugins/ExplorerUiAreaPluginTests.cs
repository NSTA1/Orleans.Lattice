using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema;
using Orleans.Lattice.Explorer.Plugins.Schema.Components;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The three area plugins the Explorer ships - Backups, Access and Schema, each
/// from its own plugin package now - and the registration surface a head uses to
/// choose which of them it surfaces. Withholding an area is registering nothing,
/// which is what the retired per-area navigation flag was emulating.
/// </summary>
[TestFixture]
public sealed class ExplorerUiAreaPluginTests
{
    [Test]
    public void Every_area_plugin_declares_a_stable_dotted_id_and_the_area_surface()
    {
        var plugins = new IExplorerPlugin[]
        {
            new BackupsAreaPlugin(Substitute.For<IBackupCapabilityService>()),
            new AccessAreaPlugin(Substitute.For<IAuthAdminCapabilityService>()),
            new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>()),
        };

        Assert.Multiple(() =>
        {
            foreach (var plugin in plugins)
            {
                Assert.That(plugin.Descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
                Assert.That(
                    plugin.Descriptor.PluginId,
                    Does.StartWith("orleans.lattice."),
                    "a package-owned id is what stops two independently authored plugins colliding");
                Assert.That(plugin.AccessGate, Is.Not.Null);
                Assert.That(plugin.ViewType, Is.Not.Null);
            }
        });
    }

    [Test]
    public void Each_area_plugin_renders_its_own_panel_and_gates_on_its_own_feature_service()
    {
        var backupGate = Substitute.For<IBackupCapabilityService>();
        var accessGate = Substitute.For<IAuthAdminCapabilityService>();
        var schemaGate = Substitute.For<ISchemaAdminCapabilityService>();

        var backups = new BackupsAreaPlugin(backupGate);
        var access = new AccessAreaPlugin(accessGate);
        var schema = new SchemaAreaPlugin(schemaGate);

        Assert.Multiple(() =>
        {
            Assert.That(backups.Descriptor.PluginId, Is.EqualTo(BackupsPluginKeys.PluginId));
            Assert.That(backups.Descriptor.Label, Is.EqualTo("Backups"));
            Assert.That(backups.ViewType, Is.EqualTo(typeof(BackupsPanel)));
            Assert.That(backups.AccessGate, Is.SameAs(backupGate));

            Assert.That(access.Descriptor.PluginId, Is.EqualTo(AccessPluginKeys.PluginId));
            Assert.That(access.Descriptor.Label, Is.EqualTo("Access"));
            Assert.That(access.ViewType, Is.EqualTo(typeof(AccessPanel)));
            Assert.That(access.AccessGate, Is.SameAs(accessGate));

            Assert.That(schema.Descriptor.PluginId, Is.EqualTo(SchemaPluginKeys.PluginId));
            Assert.That(schema.Descriptor.Label, Is.EqualTo("Schema"));
            Assert.That(schema.ViewType, Is.EqualTo(typeof(SchemaPanel)));
            Assert.That(schema.AccessGate, Is.SameAs(schemaGate));
        });
    }

    [Test]
    public void The_area_plugins_sort_backups_then_access_then_schema()
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
    public void Every_area_plugin_now_declares_its_controlled_domain_contract()
    {
        // All three areas are converted, so each one's reach is a compile-time
        // fact: the host resolves exactly the declared contract for it and
        // nothing else. The contract is supplied by IExplorerPlugin<TDomain>, so
        // it reads through the interface rather than off the concrete type.
        IExplorerPlugin backups = new BackupsAreaPlugin(Substitute.For<IBackupCapabilityService>());
        IExplorerPlugin access = new AccessAreaPlugin(Substitute.For<IAuthAdminCapabilityService>());
        IExplorerPlugin schema = new SchemaAreaPlugin(Substitute.For<ISchemaAdminCapabilityService>());

        Assert.Multiple(() =>
        {
            Assert.That(backups.DomainContract, Is.Not.Null);
            Assert.That(access.DomainContract, Is.EqualTo(typeof(IAccessDomain)));
            Assert.That(schema.DomainContract, Is.EqualTo(typeof(ISchemaPluginDomain)));
        });
    }

    [Test]
    public void The_converted_access_plugin_declares_its_controlled_domain_contract()
    {
        // Access has been converted, so its reach is a compile-time fact: the
        // host resolves IAccessDomain for it and nothing else. The contract is
        // supplied by IExplorerPlugin<TDomain>, so it reads through the
        // interface rather than off the concrete type.
        IExplorerPlugin access = new AccessAreaPlugin(Substitute.For<IAuthAdminCapabilityService>());

        Assert.That(access.DomainContract, Is.EqualTo(typeof(IAccessDomain)));
    }

    [Test]
    public void Every_area_plugin_rejects_a_null_gate()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new BackupsAreaPlugin(null!), Throws.ArgumentNullException);
            Assert.That(() => new AccessAreaPlugin(null!), Throws.ArgumentNullException);
            Assert.That(() => new SchemaAreaPlugin(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Every_registration_helper_rejects_a_null_service_collection()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ((IServiceCollection)null!).AddExplorerPluginAdapters(),
                Throws.ArgumentNullException);
            Assert.That(() => ((IServiceCollection)null!).AddExplorerBackupsPlugin(), Throws.ArgumentNullException);
            Assert.That(() => ((IServiceCollection)null!).AddExplorerAccessPlugin(), Throws.ArgumentNullException);
            Assert.That(() => ((IServiceCollection)null!).AddExplorerSchemaPlugin(), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task AddExplorerPluginAdapters_registers_the_two_head_supplied_adapters()
    {
        await using var provider = BuildHost(services => services.AddExplorerPluginAdapters());
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginHostState>(),
                Is.TypeOf<ExplorerPluginHostState>());
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginPreferences>(),
                Is.TypeOf<ExplorerPluginPreferences>());

            // The shell drives the deterministic tenant-scope refresh, so it
            // needs the concrete adapter, and both resolutions are the same
            // per-circuit instance.
            Assert.That(
                scope.ServiceProvider.GetRequiredService<ExplorerPluginHostState>(),
                Is.SameAs(scope.ServiceProvider.GetRequiredService<IExplorerPluginHostState>()));
        });
    }

    [Test]
    public async Task Registering_one_area_plugin_surfaces_exactly_that_area()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerBackup();
            services.AddExplorerBackupsPlugin();
        });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSurface(ExplorerPluginSurface.Area)
                .Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { BackupsPluginKeys.PluginId }));
    }

    [Test]
    public async Task Registering_the_same_area_plugin_twice_is_a_no_op()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerBackup();
            services.AddExplorerBackupsPlugin();
            services.AddExplorerBackupsPlugin();
        });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(),
            Is.EqualTo(1),
            "a duplicate registration must not fail the catalog's unique-id check");
    }

    private static ServiceProvider BuildHost(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        configure(services);
        return services.BuildServiceProvider();
    }
}

/// <summary>
/// The minimum Explorer graph the plugin adapters and the feature clients read
/// through to, stubbed so a registration test never opens a connection.
/// </summary>
internal static class PluginAdapterHostStubs
{
    public static IServiceCollection AddExplorerCatalogStub(this IServiceCollection services)
    {
        var connection = Substitute.For<Core.Connection.ILatticeStateConnection>();
        connection.Status.Returns(Core.Connection.LatticeConnectionStatus.Disconnected);

        services.AddSingleton(Substitute.For<Core.Catalog.IExplorerSelection>());
        services.AddSingleton(connection);
        services.AddSingleton(Substitute.For<Core.Session.IUiPreferenceStore>());
        services.AddSingleton(Substitute.For<Core.Configuration.IExplorerSession>());
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        return services;
    }
}
