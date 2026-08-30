using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeApiTelemetryServiceCollectionExtensions.AddLatticeTelemetryBackend"/>:
/// it registers the metric-access policy and the default backend proxy, honours a
/// host-supplied proxy, is idempotent, wires the configured base address and
/// timeout onto the backend <see cref="HttpClient"/>, and validates its argument.
/// </summary>
[TestFixture]
public sealed class LatticeApiTelemetryServiceCollectionExtensionsTests
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
    public void AddLatticeTelemetryBackend_registers_the_default_backend_client()
    {
        var services = Configured();
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IPrometheusQueryClient>(),
            Is.InstanceOf<PrometheusQueryClient>());
    }

    [Test]
    public void AddLatticeTelemetryBackend_defers_to_a_host_supplied_backend_client()
    {
        var custom = Substitute.For<IPrometheusQueryClient>();
        var services = Configured();
        services.AddSingleton(custom);
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IPrometheusQueryClient>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeTelemetryBackend_is_idempotent()
    {
        var services = Configured();
        services.AddLatticeTelemetryBackend();
        services.AddLatticeTelemetryBackend();

        Assert.Multiple(() =>
        {
            Assert.That(services.Count(d => d.ServiceType == typeof(IPrometheusQueryClient)), Is.EqualTo(1));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(TelemetryMetricAccessPolicy)),
                Is.EqualTo(1));
        });
    }

    [Test]
    public void AddLatticeTelemetryBackend_registers_the_metric_access_policy_from_the_bound_options()
    {
        var services = Configured(options =>
        {
            options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
            options.AllowedMetrics.Add("lattice_wal_*");
        });
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();
        var policy = provider.GetRequiredService<TelemetryMetricAccessPolicy>();

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsReadAll, Is.False);
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
            Assert.That(policy.IsAdmitted("up"), Is.False);
        });
    }

    [Test]
    public void The_metric_access_policy_is_a_singleton_so_patterns_compile_once()
    {
        var services = Configured();
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<TelemetryMetricAccessPolicy>(),
            Is.SameAs(provider.GetRequiredService<TelemetryMetricAccessPolicy>()));
    }

    [Test]
    public void The_backend_client_is_pointed_at_the_configured_address_with_the_configured_timeout()
    {
        var services = Configured(options => options.RequestTimeout = TimeSpan.FromSeconds(7));
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();
        var client = BackendHttpClient(provider);

        Assert.Multiple(() =>
        {
            Assert.That(client.BaseAddress, Is.EqualTo(new Uri("https://prometheus.internal:9090/")));
            Assert.That(client.Timeout, Is.EqualTo(TimeSpan.FromSeconds(7)));
        });
    }

    [Test]
    public void An_unset_backend_address_leaves_the_client_address_unset_rather_than_throwing()
    {
        var services = new ServiceCollection();
        services.Configure<LatticeTelemetryOptions>(_ => { });
        services.AddLatticeTelemetryBackend();

        using var provider = services.BuildServiceProvider();
        var client = BackendHttpClient(provider);

        Assert.Multiple(() =>
        {
            Assert.That(client.BaseAddress, Is.Null);

            // The default request timeout still lands, which is what proves the
            // named client was configured at all rather than handed back raw.
            Assert.That(client.Timeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void AddLatticeTelemetryBackend_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => ((IServiceCollection)null!).AddLatticeTelemetryBackend());

    [Test]
    public void AddLatticeTelemetryBackend_returns_the_service_collection_for_chaining()
    {
        var services = Configured();
        Assert.That(services.AddLatticeTelemetryBackend(), Is.SameAs(services));
    }

    /// <summary>
    /// Resolves the typed-client <see cref="HttpClient"/> the registration
    /// configured. The typed-client name is derived from the service type, so it
    /// is read back the same way rather than hard-coded.
    /// </summary>
    private static HttpClient BackendHttpClient(IServiceProvider provider)
        => provider.GetRequiredService<IHttpClientFactory>()
            .CreateClient(nameof(IPrometheusQueryClient));
}
