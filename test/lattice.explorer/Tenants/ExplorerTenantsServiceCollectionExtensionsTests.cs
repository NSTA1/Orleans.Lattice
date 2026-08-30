using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Views;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The Tenants plugin's registration: what it wires, and the composition-time
/// refusal that stops an operator-only area being composed against a gate that
/// denies everyone.
/// <para>
/// The refusal is the point of this fixture. The tenant-view seam registers a
/// fail-closed operator gate with <c>TryAdd</c>, so a head that enables tenant
/// scoping before registering a real gate silently loses the race and ships an
/// area no operator can ever open, with no error anywhere to explain it.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerTenantsServiceCollectionExtensionsTests
{
    /// <summary>A stand-in for whatever administrative surface supplies the real gate.</summary>
    private sealed class StubOperatorGate : IExplorerTenantOperatorGate
    {
        public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
            new(true);
    }

    [Test]
    public void AddExplorerTenants_null_services_throws()
    {
        Assert.That(
            () => ExplorerTenantsServiceCollectionExtensions.AddExplorerTenants(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerTenantsPlugin_null_services_throws()
    {
        Assert.That(
            () => ExplorerTenantsServiceCollectionExtensions.AddExplorerTenantsPlugin(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerTenants_without_any_operator_gate_refuses_to_compose()
    {
        var services = new ServiceCollection();

        Assert.That(
            () => services.AddExplorerTenants(),
            Throws.InvalidOperationException.With.Message.EqualTo(
                ExplorerTenantsServiceCollectionExtensions.MissingGateMessage));
    }

    [Test]
    public void AddExplorerTenants_after_the_tenant_view_alone_refuses_to_compose()
    {
        // The exact misordering: the tenant view registered its fail-closed
        // default, so any real gate registered later loses the TryAdd and every
        // caller is denied for ever.
        var services = new ServiceCollection();
        services.AddExplorerTenantView();

        Assert.That(
            () => services.AddExplorerTenants(),
            Throws.InvalidOperationException.With.Message.EqualTo(
                ExplorerTenantsServiceCollectionExtensions.MisorderedGateMessage));
    }

    [Test]
    public void AddExplorerTenants_after_a_shadowed_real_gate_refuses_to_compose()
    {
        // The head called the tenant view first and its administrative surface
        // second, so the surface's TryAdd was a no-op and the fail-closed gate is
        // still what resolves.
        var services = new ServiceCollection();
        services.AddExplorerTenantView();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();

        Assert.That(
            () => services.AddExplorerTenants(),
            Throws.InvalidOperationException.With.Message.EqualTo(
                ExplorerTenantsServiceCollectionExtensions.MisorderedGateMessage));
    }

    [Test]
    public void AddExplorerTenants_after_a_real_gate_composes()
    {
        var services = new ServiceCollection();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();

        Assert.That(() => services.AddExplorerTenants(), Throws.Nothing);
    }

    [Test]
    public void AddExplorerTenants_accepts_a_gate_registered_as_an_instance()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IExplorerTenantOperatorGate>(new StubOperatorGate());

        Assert.That(() => services.AddExplorerTenants(), Throws.Nothing);
    }

    [Test]
    public void AddExplorerTenants_reads_the_gate_that_would_actually_resolve()
    {
        // The last registration wins in the container, so a head that replaced
        // the fail-closed default outright is composable even though an earlier
        // factory registration is still in the collection.
        var services = new ServiceCollection();
        services.AddExplorerTenantView();
        services.AddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();

        Assert.That(() => services.AddExplorerTenants(), Throws.Nothing);
    }

    [Test]
    public void AddExplorerTenants_registers_the_tenant_view_the_tenancy_seam_reads()
    {
        var services = Composed();

        Assert.Multiple(() =>
        {
            Assert.That(Registered(services, typeof(IExplorerTenantSwitcher)), Is.True);
            Assert.That(Registered(services, typeof(IExplorerTenantContext)), Is.True);
        });
    }

    [Test]
    public void AddExplorerTenants_registers_the_shared_tenancy_seam_and_its_domain()
    {
        var services = Composed();

        Assert.Multiple(() =>
        {
            Assert.That(Registered(services, typeof(ITenancyDomain)), Is.True);
            Assert.That(Registered(services, typeof(ITenantAdminService)), Is.True);
            Assert.That(Registered(services, typeof(ITenancyAvailability)), Is.True);
        });
    }

    [Test]
    public void AddExplorerTenants_registers_the_plugin_gate()
    {
        var services = Composed();

        Assert.That(Registered(services, typeof(TenantsAccessGate)), Is.True);
    }

    [Test]
    public void AddExplorerTenants_does_not_register_the_plugin()
    {
        // Registering the feature and surfacing the area are separate opt-ins,
        // exactly as they are for Backups and Access.
        var services = Composed();

        Assert.That(Registered(services, typeof(IExplorerPlugin)), Is.False);
    }

    [Test]
    public void AddExplorerTenantsPlugin_registers_the_area_plugin()
    {
        var services = new ServiceCollection();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();
        services.AddExplorerTenantsPlugin();

        var descriptors = services.Where(d => d.ServiceType == typeof(IExplorerPlugin)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(descriptors, Has.Length.EqualTo(1));
            Assert.That(descriptors[0].ImplementationType, Is.EqualTo(typeof(TenantsAreaPlugin)));
        });
    }

    [Test]
    public void AddExplorerTenantsPlugin_is_idempotent()
    {
        var services = new ServiceCollection();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();
        services.AddExplorerTenantsPlugin();
        services.AddExplorerTenantsPlugin();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IExplorerPlugin)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddExplorerTenants_resolves_the_plugin_and_its_gate_from_the_container()
    {
        var services = new ServiceCollection();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();
        services.AddExplorerTenantsPlugin();

        // The tenancy client the seam registers needs an endpoint and a session
        // this test has no business standing up, so the domain is substituted
        // outright: what is under test is that the plugin graph resolves.
        services.AddScoped<ITenancyDomain>(_ => new FakeTenancyDomain());

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var plugin = scope.ServiceProvider.GetServices<IExplorerPlugin>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(plugin, Is.TypeOf<TenantsAreaPlugin>());
            Assert.That(plugin.AccessGate, Is.TypeOf<TenantsAccessGate>());
            Assert.That(plugin.DomainContract, Is.EqualTo(typeof(ITenancyDomain)));
            Assert.That(plugin.ViewType, Is.EqualTo(typeof(TenantsPanel)));
        });
    }

    private static ServiceCollection Composed()
    {
        var services = new ServiceCollection();
        services.TryAddScoped<IExplorerTenantOperatorGate, StubOperatorGate>();
        services.AddExplorerTenants();
        return services;
    }

    private static bool Registered(IServiceCollection services, Type serviceType)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (services[i].ServiceType == serviceType)
            {
                return true;
            }
        }

        return false;
    }
}
