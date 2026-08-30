using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the tenancy seam's registration: the four services it contributes, the
/// per-circuit lifetime they all take, and the two shapes a head can be in -
/// with the tenant-view seam registered, and without it, where the domain must
/// still resolve and report tenancy disabled rather than failing to construct.
/// </summary>
[TestFixture]
public class ExplorerTenancyServiceCollectionExtensionsTests
{
    [Test]
    public void Add_rejects_a_null_service_collection() =>
        Assert.That(
            () => ExplorerTenancyServiceCollectionExtensions.AddExplorerTenancy(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Add_registers_the_seam_as_scoped_per_circuit()
    {
        var services = new ServiceCollection().AddExplorerTenancy();

        Assert.Multiple(() =>
        {
            AssertScoped(services, typeof(ITenantAdminClient), typeof(GrpcTenantAdminClient));
            AssertScoped(services, typeof(ITenantAdminService), typeof(TenantAdminService));
            AssertScoped(services, typeof(ITenancyAvailability), typeof(TenancyAvailability));
            AssertScoped(services, typeof(ITenancyDomain), typeof(TenancyDomain));
        });
    }

    [Test]
    public void Add_is_idempotent_and_never_replaces_a_head_supplied_client()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());

        services.AddExplorerTenancy().AddExplorerTenancy();

        var clients = services.Where(d => d.ServiceType == typeof(ITenantAdminClient)).ToArray();
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

        var domain = scope.ServiceProvider.GetRequiredService<ITenancyDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(domain, Is.InstanceOf<TenancyDomain>());
            Assert.That(domain.IsTenancyEnabled, Is.False);
            Assert.That(domain.ActiveTenant, Is.Null);
            Assert.That(domain.Tenants, Is.InstanceOf<TenantAdminService>());
        });
    }

    [Test]
    public async Task Without_the_tenant_view_the_domain_probe_reports_unavailable()
    {
        using var provider = BuildProvider(withTenantView: false);
        using var scope = provider.CreateScope();

        var access = await scope.ServiceProvider.GetRequiredService<ITenancyDomain>().ProbeAvailabilityAsync();

        Assert.That(access.IsVisible, Is.False, "a tenancy plugin renders nothing without the tenancy add-on");
    }

    [Test]
    public void With_the_tenant_view_the_domain_picks_up_the_existing_switcher()
    {
        using var provider = BuildProvider(withTenantView: true);
        using var scope = provider.CreateScope();

        var domain = scope.ServiceProvider.GetRequiredService<ITenancyDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(domain.IsTenancyEnabled, Is.True);
            Assert.That(domain.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
        });
    }

    [Test]
    public void The_seam_is_resolved_once_per_scope_and_not_shared_across_scopes()
    {
        using var provider = BuildProvider(withTenantView: true);
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        var a = first.ServiceProvider.GetRequiredService<ITenancyDomain>();
        var b = first.ServiceProvider.GetRequiredService<ITenancyDomain>();
        var c = second.ServiceProvider.GetRequiredService<ITenancyDomain>();

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.SameAs(b));
            Assert.That(a, Is.Not.SameAs(c));
        });
    }

    private static ServiceProvider BuildProvider(bool withTenantView)
    {
        var services = new ServiceCollection();

        // Registered before the seam so its TryAdd leaves it in place: the real
        // client needs a live session and sign-in, which this test has no use for.
        services.AddScoped<ITenantAdminClient>(_ => new FakeTenantAdminClient());
        if (withTenantView)
        {
            services.AddScoped<IExplorerTenantSwitcher>(_ => new StubTenantSwitcher());
        }

        return services.AddExplorerTenancy().BuildServiceProvider();
    }

    private static void AssertScoped(IServiceCollection services, Type serviceType, Type implementationType)
    {
        var descriptor = services.SingleOrDefault(d => d.ServiceType == serviceType);

        Assert.That(descriptor, Is.Not.Null, $"{serviceType.Name} was not registered");
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        Assert.That(descriptor.ImplementationType, Is.EqualTo(implementationType));
    }
}
