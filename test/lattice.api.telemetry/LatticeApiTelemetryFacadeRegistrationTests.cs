using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeApiTelemetryServiceCollectionExtensions.AddLatticeTelemetryApi"/>:
/// it composes the facade over the backend registration, is idempotent, resolves
/// the optional core seams when they are present, and still produces a working,
/// fail-closed facade when they are not.
/// </summary>
[TestFixture]
public sealed class LatticeApiTelemetryFacadeRegistrationTests
{
    private static ServiceCollection Configured(Action<LatticeTelemetryOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.Configure<LatticeTelemetryOptions>(options =>
        {
            options.BackendAddress = new Uri("https://prometheus.internal:9090/");
            configure?.Invoke(options);
        });

        return services;
    }

    [Test]
    public void AddLatticeTelemetryApi_registers_the_facade_and_its_collaborators()
    {
        var services = Configured();
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<ILatticeTelemetry>(), Is.InstanceOf<LatticeTelemetry>());
            Assert.That(provider.GetRequiredService<LatticeTelemetryQueryCatalog>(), Is.Not.Null);
            Assert.That(provider.GetRequiredService<TelemetryTenantScopeResolver>(), Is.Not.Null);
            Assert.That(provider.GetRequiredService<TelemetryAccessAuthorizer>(), Is.Not.Null);
        });
    }

    [Test]
    public void AddLatticeTelemetryApi_also_registers_the_backend_proxy_and_policy()
    {
        var services = Configured();
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IPrometheusQueryClient>(), Is.Not.Null);
            Assert.That(provider.GetRequiredService<TelemetryMetricAccessPolicy>(), Is.Not.Null);
        });
    }

    [Test]
    public void AddLatticeTelemetryApi_is_idempotent()
    {
        var services = Configured();
        services.AddLatticeTelemetryApi();
        services.AddLatticeTelemetryApi();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeTelemetry)), Is.EqualTo(1));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(LatticeTelemetryQueryCatalog)),
                Is.EqualTo(1));
            Assert.That(services.Count(d => d.ServiceType == typeof(IPrometheusQueryClient)), Is.EqualTo(1));
        });
    }

    [Test]
    public void AddLatticeTelemetryApi_builds_the_catalogue_from_the_configured_allow_list()
    {
        var services = Configured(options =>
        {
            options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
            options.AllowedMetrics.Add("orleans_lattice_storage_total_bytes");
        });
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<LatticeTelemetryQueryCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Offers("tree.storage.bytes"), Is.True);
            Assert.That(catalog.Offers("tree.read.operation_rate"), Is.False);
        });
    }

    [Test]
    public void AddLatticeTelemetryApi_consumes_a_host_registered_tenant_resolver()
    {
        var resolver = new StubTenantContextResolver(TenantId.Parse("acme"));
        var services = Configured();
        services.AddSingleton<ITenantContextResolver>(resolver);
        services.AddSingleton(Substitute.For<IPrometheusQueryClient>());
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();
        var scope = provider.GetRequiredService<TelemetryTenantScopeResolver>()
            .ResolveAsync(TelemetryTenantVisibility.ActiveTenant)
            .AsTask()
            .GetAwaiter()
            .GetResult();

        Assert.That(scope.TenantId, Is.EqualTo("acme"));
    }

    [Test]
    public void AddLatticeTelemetryApi_falls_back_to_the_default_tenant_when_no_resolver_is_registered()
    {
        var services = Configured();
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();
        var scope = provider.GetRequiredService<TelemetryTenantScopeResolver>()
            .ResolveAsync(TelemetryTenantVisibility.ActiveTenant)
            .AsTask()
            .GetAwaiter()
            .GetResult();

        Assert.That(scope.TenantId, Is.EqualTo(LatticeTenantLabel.DefaultTenant),
            "A minimal host still gets a bounded scope rather than an unscoped one.");
    }

    [Test]
    public void AddLatticeTelemetryApi_fails_closed_on_widening_when_no_access_gate_is_registered()
    {
        var services = Configured();
        services.AddLatticeTelemetryApi();

        using var provider = services.BuildServiceProvider();
        var scope = provider.GetRequiredService<TelemetryTenantScopeResolver>()
            .ResolveAsync(TelemetryTenantVisibility.AllTenants)
            .AsTask()
            .GetAwaiter()
            .GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(scope.WasDowngraded, Is.True);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public void AddLatticeTelemetryApi_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddLatticeTelemetryApi(),
            Throws.ArgumentNullException);
    }
}
