using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Tests.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The plugin's registration: what <c>AddExplorerMyTenant()</c> brings with it,
/// what <c>AddExplorerMyTenantPlugin()</c> surfaces, and - the part that bites -
/// the ordering between the tenant-view seam and the administrative surface that
/// supplies the real platform-operator gate.
/// </summary>
[TestFixture]
public sealed class ExplorerMyTenantServiceCollectionExtensionsTests
{
    private sealed class StubMetricsSection : IMyTenantMetricsSection
    {
        public Type ViewType => typeof(StubMetricsSection);

        public string Label => "Metrics";
    }

    [Test]
    public void The_feature_registration_brings_the_tenancy_seam_with_it()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenant();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(ITenancyDomain)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IMyTenantDomain)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ITenantAdminService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IMyTenantAccessGate)), Is.True);
        });
    }

    [Test]
    public void The_feature_registration_brings_the_plugin_host_with_it()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenant();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerPluginAccessStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerPluginHostContextFactory)), Is.True);
        });
    }

    [Test]
    public void Everything_is_scoped_per_circuit()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenant();

        Assert.That(
            services.Single(d => d.ServiceType == typeof(IMyTenantAccessGate)).Lifetime,
            Is.EqualTo(ServiceLifetime.Scoped),
            "the gate reads the calling scope's tenant identity, so it must not be shared");
    }

    [Test]
    public void Registering_the_feature_twice_is_idempotent()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenant();
        services.AddExplorerMyTenant();

        Assert.That(services.Count(d => d.ServiceType == typeof(IMyTenantAccessGate)), Is.EqualTo(1));
    }

    [Test]
    public void The_plugin_registration_surfaces_exactly_one_area()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenantPlugin();
        services.AddExplorerMyTenantPlugin();

        Assert.That(services.Count(d => d.ServiceType == typeof(IExplorerPlugin)), Is.EqualTo(1));
    }

    [Test]
    public void A_head_that_registers_neither_ships_no_my_tenant_area()
    {
        var services = new ServiceCollection();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerPlugin)), Is.False);
            Assert.That(services.Any(d => d.ServiceType == typeof(IMyTenantAccessGate)), Is.False);
        });
    }

    [Test]
    public void The_metrics_section_seam_is_optional_and_registerable()
    {
        var services = new ServiceCollection();

        Assert.That(services.Any(d => d.ServiceType == typeof(IMyTenantMetricsSection)), Is.False);

        services.AddExplorerMyTenantMetricsSection<StubMetricsSection>();

        Assert.That(services.Count(d => d.ServiceType == typeof(IMyTenantMetricsSection)), Is.EqualTo(1));
    }

    [Test]
    public void The_metrics_accessor_is_registered_so_the_panel_needs_no_service_provider()
    {
        var services = new ServiceCollection();

        services.AddExplorerMyTenant();

        Assert.That(
            services.Single(d => d.ServiceType == typeof(MyTenantMetricsSectionAccessor)).Lifetime,
            Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void The_metrics_accessor_resolves_with_no_section_registered()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        services.AddExplorerMyTenant();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<MyTenantMetricsSectionAccessor>().HasSection,
            Is.False);
    }

    [Test]
    public void The_metrics_accessor_picks_up_a_registered_section()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        services.AddExplorerMyTenant();
        services.AddExplorerMyTenantMetricsSection<StubMetricsSection>();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<MyTenantMetricsSectionAccessor>().Section,
            Is.InstanceOf<StubMetricsSection>());
    }

    [Test]
    public void Null_service_collections_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => ExplorerMyTenantServiceCollectionExtensions.AddExplorerMyTenant(null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => ExplorerMyTenantServiceCollectionExtensions.AddExplorerMyTenantPlugin(null!),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => ExplorerMyTenantServiceCollectionExtensions
                    .AddExplorerMyTenantMetricsSection<StubMetricsSection>(null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    [Test]
    public void The_correctly_ordered_head_gets_the_real_platform_operator_gate()
    {
        // Access first, then the tenant view: the real gate wins the TryAdd.
        using var provider = BuildHead(accessFirst: true).BuildServiceProvider();
        using var scope = provider.CreateScope();
        var gate = scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>();

        Assert.Multiple(() =>
        {
            Assert.That(MyTenantOperatorGateDiagnostic.IsFailClosedPlaceholder(gate), Is.False);
            Assert.That(MyTenantOperatorGateDiagnostic.Describe(gate), Is.Null);
        });
    }

    [Test]
    public void The_misordered_head_silently_keeps_the_fail_closed_placeholder()
    {
        // The tenant view first: TryAdd keeps its placeholder, and the real gate
        // registered afterwards never wins. Nothing throws and nothing warns -
        // which is exactly the failure mode the plugin's diagnostic exists for.
        using var provider = BuildHead(accessFirst: false).BuildServiceProvider();
        using var scope = provider.CreateScope();
        var gate = scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>();

        Assert.Multiple(() =>
        {
            Assert.That(
                MyTenantOperatorGateDiagnostic.IsFailClosedPlaceholder(gate),
                Is.True,
                "the misordering is real and silent, so the plugin has to detect it");
            Assert.That(
                MyTenantOperatorGateDiagnostic.Describe(gate),
                Is.EqualTo(MyTenantOperatorGateDiagnostic.PlaceholderGateMessage));
        });
    }

    [Test]
    public void The_plugin_resolves_from_a_correctly_ordered_head()
    {
        var services = BuildHead(accessFirst: true);
        services.AddExplorerMyTenantPlugin();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var plugins = scope.ServiceProvider.GetServices<IExplorerPlugin>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(plugins, Has.Length.EqualTo(1));
            Assert.That(plugins[0], Is.InstanceOf<MyTenantAreaPlugin>());
            Assert.That(plugins[0].Descriptor.PluginId, Is.EqualTo(MyTenantPluginKeys.PluginId));
            Assert.That(plugins[0].DomainContract, Is.EqualTo(typeof(IMyTenantDomain)));
            Assert.That(plugins[0].AccessGate, Is.InstanceOf<IMyTenantAccessGate>());
        });
    }

    [Test]
    public void The_gate_resolves_on_a_head_that_never_opted_into_tenant_scoping()
    {
        // No AddExplorerTenantView: the graph must still resolve, because the
        // non-tenant posture is legal and simply renders nothing.
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        services.AddExplorerMyTenant();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            () => scope.ServiceProvider.GetRequiredService<IMyTenantAccessGate>(),
            Throws.Nothing);
    }

    [Test]
    public void The_surface_is_unavailable_on_a_head_that_never_opted_into_tenant_scoping()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        services.AddExplorerMyTenant();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var domain = scope.ServiceProvider.GetRequiredService<ITenancyDomain>();

        Assert.That(
            domain.IsTenancyEnabled,
            Is.False,
            "no tenant view means no tenancy here, so the area renders nothing (D9)");
    }

    /// <summary>
    /// Builds the registrations of a head, in the given order.
    /// </summary>
    /// <remarks>
    /// The tenant-administration client is pre-registered so the seam's
    /// <c>TryAdd</c> leaves it in place: the real one needs a live session and
    /// sign-in, which a registration test has no use for.
    /// </remarks>
    private static ServiceCollection BuildHead(bool accessFirst)
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());

        if (accessFirst)
        {
            services.AddExplorerAccess();
            services.AddExplorerTenantView();
        }
        else
        {
            services.AddExplorerTenantView();
            services.AddExplorerAccess();
        }

        services.AddExplorerMyTenant();
        return services;
    }
}
