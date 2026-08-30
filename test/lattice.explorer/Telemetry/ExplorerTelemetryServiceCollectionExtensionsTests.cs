using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Telemetry;
using Orleans.Lattice.Explorer.Tests.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers the telemetry seam's registration: the four services it contributes,
/// the per-circuit lifetime they all take, and the two shapes a head can be in -
/// with the tenant-view seam registered, and without it, where the domain must
/// still resolve and report tenancy disabled rather than failing to construct.
/// </summary>
[TestFixture]
public class ExplorerTelemetryServiceCollectionExtensionsTests
{
    [Test]
    public void Add_rejects_a_null_service_collection() =>
        Assert.That(
            () => ExplorerTelemetryServiceCollectionExtensions.AddExplorerTelemetry(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Add_registers_the_seam_as_scoped_per_circuit()
    {
        var services = new ServiceCollection().AddExplorerTelemetry();

        Assert.Multiple(() =>
        {
            AssertScoped(services, typeof(ITelemetryQueryClient), typeof(GrpcTelemetryQueryClient));
            AssertScoped(services, typeof(ITelemetryQueryService), typeof(TelemetryQueryService));
            AssertScoped(services, typeof(ITelemetryAvailability), typeof(TelemetryAvailability));
            AssertScoped(services, typeof(ITelemetryDomain), typeof(TelemetryDomain));
        });
    }

    [Test]
    public void Add_is_idempotent_and_never_replaces_a_head_supplied_client()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITelemetryQueryClient>(_ => new FakeTelemetryQueryClient());

        services.AddExplorerTelemetry().AddExplorerTelemetry();

        var clients = services.Where(d => d.ServiceType == typeof(ITelemetryQueryClient)).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(clients, Has.Length.EqualTo(1));
            Assert.That(clients[0].ImplementationType, Is.Null, "the head's own factory registration must win");
        });
    }

    [Test]
    public void The_domain_resolves_without_the_tenant_view_and_reports_tenancy_disabled()
    {
        using var provider = BuildProvider(withTenantView: false);
        using var scope = provider.CreateScope();

        var domain = scope.ServiceProvider.GetRequiredService<ITelemetryDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(domain, Is.InstanceOf<TelemetryDomain>());
            Assert.That(domain.IsTenancyEnabled, Is.False);
            Assert.That(domain.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(domain.Queries, Is.InstanceOf<TelemetryQueryService>());
        });
    }

    [Test]
    public void With_the_tenant_view_the_domain_picks_up_the_existing_switcher()
    {
        using var provider = BuildProvider(withTenantView: true);
        using var scope = provider.CreateScope();

        var domain = scope.ServiceProvider.GetRequiredService<ITelemetryDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(domain.IsTenancyEnabled, Is.True);
            Assert.That(domain.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
        });
    }

    [Test]
    public async Task The_resolved_domain_reads_the_catalogue_through_the_registered_client()
    {
        using var provider = BuildProvider(withTenantView: false);
        using var scope = provider.CreateScope();

        var access = await scope.ServiceProvider.GetRequiredService<ITelemetryDomain>().ProbeAvailabilityAsync();

        Assert.That(access.IsAllowed, Is.True);
    }

    [Test]
    public void The_seam_is_resolved_once_per_scope_and_not_shared_across_scopes()
    {
        using var provider = BuildProvider(withTenantView: true);
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        var a = first.ServiceProvider.GetRequiredService<ITelemetryDomain>();
        var b = first.ServiceProvider.GetRequiredService<ITelemetryDomain>();
        var c = second.ServiceProvider.GetRequiredService<ITelemetryDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.SameAs(b));
            Assert.That(a, Is.Not.SameAs(c), "one circuit's remembered catalogue must not serve another's");
        });
    }

    [Test]
    public void The_plugin_host_the_seam_depends_on_is_registered_too()
    {
        var services = new ServiceCollection().AddExplorerTelemetry();

        Assert.That(
            services.Any(d => d.ServiceType == typeof(IExplorerPluginCatalog)),
            Is.True);
    }

    private static ServiceProvider BuildProvider(bool withTenantView)
    {
        var services = new ServiceCollection();

        // Registered before the seam so its TryAdd leaves it in place: the real
        // client needs a live session and sign-in, which this test has no use for.
        services.AddScoped<ITelemetryQueryClient>(_ => new FakeTelemetryQueryClient());
        if (withTenantView)
        {
            services.AddScoped<IExplorerTenantSwitcher>(_ => new StubTenantSwitcher
            {
                RequestedVisibility = ExplorerTenantVisibility.AllTenants,
            });
        }

        return services.AddExplorerTelemetry().BuildServiceProvider();
    }

    private static void AssertScoped(IServiceCollection services, Type serviceType, Type implementationType)
    {
        var descriptor = services.SingleOrDefault(d => d.ServiceType == serviceType);

        Assert.That(descriptor, Is.Not.Null, $"{serviceType.Name} was not registered");
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        Assert.That(descriptor.ImplementationType, Is.EqualTo(implementationType));
    }
}
