using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups area as a self-contained plugin package: its descriptor, the
/// panel it renders, the controlled domain contract it declares, the gate it
/// carries, and the registration surface a head opts in with.
/// </summary>
[TestFixture]
public sealed class BackupsAreaPluginTests
{
    [Test]
    public void The_plugin_declares_a_stable_dotted_id_on_the_area_surface()
    {
        var plugin = new BackupsAreaPlugin(Substitute.For<IBackupCapabilityService>());

        Assert.Multiple(() =>
        {
            Assert.That(plugin.Descriptor.PluginId, Is.EqualTo(BackupsPluginKeys.PluginId));
            Assert.That(
                plugin.Descriptor.PluginId,
                Does.StartWith("orleans.lattice."),
                "a package-owned id is what stops two independently authored plugins colliding");
            Assert.That(plugin.Descriptor.Label, Is.EqualTo("Backups"));
            Assert.That(plugin.Descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
        });
    }

    [Test]
    public void The_plugin_renders_its_own_panel_and_gates_on_its_own_capability_service()
    {
        var gate = Substitute.For<IBackupCapabilityService>();

        var plugin = new BackupsAreaPlugin(gate);

        Assert.Multiple(() =>
        {
            Assert.That(plugin.ViewType, Is.EqualTo(typeof(BackupsPanel)));
            Assert.That(plugin.AccessGate, Is.SameAs(gate));
        });
    }

    [Test]
    public void The_plugin_declares_the_backups_domain_as_its_controlled_contract()
    {
        IExplorerPlugin plugin = new BackupsAreaPlugin(Substitute.For<IBackupCapabilityService>());

        // The whole of what the host will resolve for Backups. Declaring it in
        // the type system is what makes the plugin's reach reviewable in
        // isolation (epic decision D3).
        Assert.That(plugin.DomainContract, Is.EqualTo(typeof(IBackupsDomain)));
    }

    [Test]
    public void The_plugin_rejects_a_null_gate()
    {
        Assert.That(() => new BackupsAreaPlugin(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerBackupsPlugin_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddExplorerBackupsPlugin(),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task AddExplorerBackup_registers_the_declared_domain_model()
    {
        var services = new ServiceCollection();
        services.AddExplorerBackup();
        services.AddSingleton<IBackupControlClient, FakeBackupControlClient>();
        services.AddSingleton(Substitute.For<Core.Catalog.ICatalogReader>());
        await using var provider = services.BuildServiceProvider();

        // The contract the plugin declares must resolve, or the panel's
        // GetDomain call fails at render time rather than at registration.
        Assert.That(provider.GetRequiredService<IBackupsDomain>(), Is.InstanceOf<BackupsDomain>());
    }

    [Test]
    public async Task AddExplorerBackupsPlugin_surfaces_exactly_the_backups_area()
    {
        await using var provider = BuildHost();
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSurface(ExplorerPluginSurface.Area)
                .Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { BackupsPluginKeys.PluginId }));
    }

    [Test]
    public async Task Registering_the_backups_plugin_twice_is_a_no_op()
    {
        await using var provider = BuildHost(registerPluginTwice: true);
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(),
            Is.EqualTo(1),
            "a duplicate registration must not fail the catalog's unique-id check");
    }

    /// <summary>
    /// The minimum Explorer graph the Backups package reads through to, stubbed
    /// so a registration test never opens a connection.
    /// </summary>
    private static ServiceProvider BuildHost(bool registerPluginTwice = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<Core.Configuration.IExplorerSession>());
        services.AddSingleton(Substitute.For<Core.Authentication.IExplorerAuthSession>());
        services.AddSingleton(Substitute.For<Core.Catalog.ICatalogReader>());
        services.AddExplorerBackup();
        services.AddExplorerBackupsPlugin();
        if (registerPluginTwice)
        {
            services.AddExplorerBackupsPlugin();
        }

        return services.BuildServiceProvider();
    }
}
