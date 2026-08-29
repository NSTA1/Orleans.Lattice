using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerPluginHost_registers_the_host_machinery()
    {
        // The head supplies the two adapters the contract deliberately does not
        // implement, exactly as a real composition does.
        var provider = Host(services =>
        {
            services.AddExplorerPluginHost();
            services.AddScoped<IExplorerPluginHostState, FakeExplorerPluginHostState>();
            services.AddScoped<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
        });

        using var scope = provider.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetService<IExplorerPluginCatalog>(), Is.TypeOf<ExplorerPluginCatalog>());
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginAccessStore>(),
                Is.TypeOf<ExplorerPluginAccessStore>());
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginAccessRefresher>(),
                Is.TypeOf<ExplorerPluginAccessRefresher>());
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginDomainResolver>(),
                Is.TypeOf<ExplorerPluginDomainResolver>());
            Assert.That(
                scope.ServiceProvider.GetService<IExplorerPluginHostContextFactory>(),
                Is.TypeOf<ExplorerPluginHostContextFactory>());
        });
    }

    [Test]
    public void The_refresher_needs_the_head_supplied_state_and_preference_adapters()
    {
        var provider = Host(services => services.AddExplorerPluginHost());

        using var scope = provider.CreateScope();

        // The contract carries no cluster dependency, so it cannot implement
        // the ambient-state and preference adapters itself. Registering the
        // host machinery alone therefore leaves the graph incomplete by design.
        Assert.That(
            () => scope.ServiceProvider.GetService<IExplorerPluginHostContextFactory>(),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Host_services_are_scoped_so_one_circuit_never_sees_anothers_decisions()
    {
        var provider = Host(services => services.AddExplorerPluginHost());

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.That(
            first.ServiceProvider.GetRequiredService<IExplorerPluginAccessStore>(),
            Is.Not.SameAs(second.ServiceProvider.GetRequiredService<IExplorerPluginAccessStore>()));
    }

    [Test]
    public void AddExplorerPluginHost_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddExplorerPluginHost();
        services.AddExplorerPluginHost();

        Assert.That(services.Count(d => d.ServiceType == typeof(IExplorerPluginAccessStore)), Is.EqualTo(1));
    }

    [Test]
    public void AddExplorerPlugin_registers_the_plugin_and_the_host_machinery()
    {
        var provider = Host(services => services.AddExplorerPlugin<SamplePlugin>());

        using var scope = provider.CreateScope();
        var catalog = scope.ServiceProvider.GetRequiredService<IExplorerPluginCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.All.Select(p => p.Descriptor.PluginId), Is.EqualTo(new[] { "sample" }).AsCollection);
            Assert.That(catalog.Find("sample"), Is.TypeOf<SamplePlugin>());
        });
    }

    [Test]
    public void Registering_the_same_plugin_type_twice_is_a_no_op()
    {
        var provider = Host(services =>
        {
            services.AddExplorerPlugin<SamplePlugin>();
            services.AddExplorerPlugin<SamplePlugin>();
        });

        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void Several_plugin_types_each_register_once()
    {
        var provider = Host(services =>
        {
            services.AddExplorerPlugin<SamplePlugin>();
            services.AddExplorerPlugin<OtherPlugin>();
        });

        using var scope = provider.CreateScope();
        var catalog = scope.ServiceProvider.GetRequiredService<IExplorerPluginCatalog>();

        Assert.That(
            catalog.All.Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { "other", "sample" }).AsCollection);
    }

    [Test]
    public void AddExplorerPlugin_instance_overload_registers_the_supplied_instance()
    {
        var plugin = new FakeExplorerPlugin("instance");
        var provider = Host(services => services.AddExplorerPlugin(plugin));

        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<IExplorerPluginCatalog>().Find("instance"), Is.SameAs(plugin));
    }

    [Test]
    public void Registering_the_same_plugin_instance_twice_is_a_no_op()
    {
        var plugin = new FakeExplorerPlugin("instance");
        var provider = Host(services =>
        {
            services.AddExplorerPlugin(plugin);
            services.AddExplorerPlugin(plugin);
        });

        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void Registration_extensions_reject_null_arguments()
    {
        var services = new ServiceCollection();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => ExplorerPluginServiceCollectionExtensions.AddExplorerPluginHost(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerPluginServiceCollectionExtensions.AddExplorerPlugin<SamplePlugin>(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerPluginServiceCollectionExtensions.AddExplorerPlugin(null!, new FakeExplorerPlugin("a")),
                Throws.ArgumentNullException);
            Assert.That(() => services.AddExplorerPlugin(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Registration_extensions_return_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();

        Assert.Multiple(() =>
        {
            Assert.That(services.AddExplorerPluginHost(), Is.SameAs(services));
            Assert.That(services.AddExplorerPlugin<SamplePlugin>(), Is.SameAs(services));
            Assert.That(services.AddExplorerPlugin(new FakeExplorerPlugin("instance")), Is.SameAs(services));
        });
    }

    [Test]
    public async Task A_registered_plugin_set_probes_end_to_end_through_the_container()
    {
        var provider = Host(services =>
        {
            services.AddExplorerPlugin<SamplePlugin>();
            services.AddExplorerPlugin<OtherPlugin>();
            services.AddSingleton<IExplorerPluginHostState, FakeExplorerPluginHostState>();
            services.AddSingleton<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
        });

        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IExplorerPluginAccessRefresher>().RefreshAsync();

        var store = scope.ServiceProvider.GetRequiredService<IExplorerPluginAccessStore>();

        Assert.Multiple(() =>
        {
            Assert.That(store.Get("sample"), Is.EqualTo(ExplorerPluginAccess.Allowed));
            Assert.That(store.Get("other"), Is.EqualTo(ExplorerPluginAccess.Unavailable));
        });
    }

    private static ServiceProvider Host(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        return services.BuildServiceProvider();
    }

    private sealed class SamplePlugin : IExplorerPlugin
    {
        public ExplorerPluginDescriptor Descriptor { get; } = new()
        {
            PluginId = "sample",
            Label = "Sample",
            Surface = ExplorerPluginSurface.Area,
        };

        public Type ViewType => typeof(SamplePlugin);

        public Type? DomainContract => null;

        public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
    }

    private sealed class OtherPlugin : IExplorerPlugin
    {
        public ExplorerPluginDescriptor Descriptor { get; } = new()
        {
            PluginId = "other",
            Label = "Other",
            Surface = ExplorerPluginSurface.Selection,
        };

        public Type ViewType => typeof(OtherPlugin);

        public Type? DomainContract => null;

        public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Unavailable;
    }
}
