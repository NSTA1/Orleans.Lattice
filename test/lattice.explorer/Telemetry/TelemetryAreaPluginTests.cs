using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Views;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The plugin descriptor the shell enumerates, the My Tenant section adapter,
/// and the registrations a head opts in with.
/// </summary>
[TestFixture]
public sealed class TelemetryAreaPluginTests
{
    private static TelemetryAreaPlugin CreatePlugin() =>
        new(new TelemetryAvailability(new TelemetryQueryService(new FakeTelemetryQueryClient())));

    [Test]
    public void The_descriptor_keys_the_plugin_on_its_own_id()
    {
        var descriptor = CreatePlugin().Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.PluginId, Is.EqualTo(TelemetryPluginKeys.PluginId));
            Assert.That(descriptor.Label, Is.EqualTo("Telemetry"));
            Assert.That(descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
        });
    }

    [Test]
    public void The_plugin_id_is_dotted_and_package_owned_so_it_cannot_collide() =>
        Assert.That(TelemetryPluginKeys.PluginId, Is.EqualTo("orleans.lattice.telemetry"));

    [Test]
    public void The_order_is_the_one_no_other_area_claims() =>
        // Backups 100, Access 200, Schema 300, Tenants 400, My Tenant 500. Two
        // areas sharing an Order hands their relative position to an arbitrary
        // tie-break; AssembledExplorerHostSmokeTests fails the whole head on it.
        Assert.That(CreatePlugin().Descriptor.Order, Is.EqualTo(600));

    [Test]
    public void The_declared_domain_contract_is_the_telemetry_seam_and_nothing_wider() =>
        // The whole reach of the surface, stated once in the plugin's signature
        // (epic decision D3).
        Assert.That(((IExplorerPlugin)CreatePlugin()).DomainContract, Is.EqualTo(typeof(ITelemetryDomain)));

    [Test]
    public void The_view_is_the_plugins_own_panel() =>
        Assert.That(CreatePlugin().ViewType, Is.EqualTo(typeof(TelemetryPanel)));

    [Test]
    public void The_gate_is_the_one_the_plugin_was_constructed_with()
    {
        var gate = new TelemetryAvailability(new TelemetryQueryService(new FakeTelemetryQueryClient()));

        Assert.That(new TelemetryAreaPlugin(gate).AccessGate, Is.SameAs(gate));
    }

    [Test]
    public void A_null_gate_is_rejected() =>
        Assert.That(() => new TelemetryAreaPlugin(null!), Throws.InstanceOf<ArgumentNullException>());

    [Test]
    public void The_descriptor_is_a_cached_instance_so_enumeration_allocates_nothing()
    {
        var plugin = CreatePlugin();

        Assert.That(plugin.Descriptor, Is.SameAs(plugin.Descriptor));
    }

    // ---- the My Tenant section adapter --------------------------------------

    [Test]
    public void The_my_tenant_section_renders_the_pinned_view_and_carries_a_label()
    {
        var section = new TelemetryMyTenantSection();

        Assert.Multiple(() =>
        {
            Assert.That(section.ViewType, Is.EqualTo(typeof(TelemetryTenantSection)));
            Assert.That(section.Label, Is.Not.Empty);
        });
    }

    [Test]
    public void The_my_tenant_section_is_an_adapter_over_the_same_package_not_a_second_implementation() =>
        // Both mounts render TelemetryBoard; the only difference is the
        // workspace the mounting component constructs.
        Assert.That(
            typeof(TelemetryTenantSection).Assembly,
            Is.SameAs(typeof(TelemetryPanel).Assembly));

    // ---- registration -------------------------------------------------------

    [Test]
    public void Adding_the_seam_registers_the_gate_as_its_own_type_so_the_plugin_can_take_it()
    {
        var services = BuildServices();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetService<TelemetryAvailability>(), Is.Not.Null);
    }

    [Test]
    public void The_gate_and_the_domain_share_one_scope_so_they_share_one_remembered_catalogue()
    {
        var services = BuildServices();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<TelemetryAvailability>(),
            Is.SameAs(scope.ServiceProvider.GetRequiredService<TelemetryAvailability>()));
    }

    [Test]
    public void Registering_the_plugin_surfaces_exactly_one_telemetry_area()
    {
        var services = BuildServices();
        services.AddExplorerTelemetryPlugin();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var areas = scope.ServiceProvider
            .GetRequiredService<IExplorerPluginCatalog>()
            .ForSurface(ExplorerPluginSurface.Area)
            .Where(plugin => plugin.Descriptor.PluginId == TelemetryPluginKeys.PluginId)
            .ToArray();

        Assert.That(areas, Has.Length.EqualTo(1));
    }

    [Test]
    public void Withholding_the_plugin_ships_no_telemetry_area_at_all()
    {
        var services = BuildServices();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSurface(ExplorerPluginSurface.Area)
                .Select(plugin => plugin.Descriptor.PluginId),
            Does.Not.Contain(TelemetryPluginKeys.PluginId));
    }

    [Test]
    public void Registering_the_my_tenant_section_fills_the_seam_my_tenant_declared()
    {
        var services = BuildServices();
        services.AddExplorerMyTenant();
        services.AddExplorerTelemetryMyTenantSection();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<MyTenantMetricsSectionAccessor>().Section,
            Is.InstanceOf<TelemetryMyTenantSection>());
    }

    [Test]
    public void Without_the_section_the_my_tenant_metrics_surface_keeps_its_placeholder()
    {
        var services = BuildServices();
        services.AddExplorerMyTenant();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<MyTenantMetricsSectionAccessor>().HasSection, Is.False);
    }

    [Test]
    public void The_area_and_the_section_are_independent_opt_ins()
    {
        // A head may take the tenant-facing section without the operator-facing
        // area, which is why they are two calls rather than one.
        var services = BuildServices();
        services.AddExplorerMyTenant();
        services.AddExplorerTelemetryMyTenantSection();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.ServiceProvider.GetRequiredService<MyTenantMetricsSectionAccessor>().HasSection,
                Is.True);
            Assert.That(
                scope.ServiceProvider
                    .GetRequiredService<IExplorerPluginCatalog>()
                    .ForSurface(ExplorerPluginSurface.Area)
                    .Select(plugin => plugin.Descriptor.PluginId),
                Does.Not.Contain(TelemetryPluginKeys.PluginId));
        });
    }

    [Test]
    public void Every_registration_rejects_a_null_collection() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ExplorerTelemetryServiceCollectionExtensions.AddExplorerTelemetryPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => ExplorerTelemetryServiceCollectionExtensions.AddExplorerTelemetryMyTenantSection(null!),
                Throws.ArgumentNullException);
        });

    private static ServiceCollection BuildServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        // The telemetry client opens a channel from the Explorer's session and
        // auth seams, neither of which a unit test has. Substituting both keeps
        // resolution from reaching the network while leaving every telemetry
        // registration real.
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        services.AddSingleton(Substitute.For<IExplorerSession>());
        services.AddExplorerTelemetry();
        return services;
    }
}
