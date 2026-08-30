using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpTelemetryServiceCollectionExtensions.AddTelemetryTools"/>.
/// Proves the opt-in registers exactly one telemetry tool group (serving the
/// telemetry group), binds and validates the options, registers the default
/// backend client overridably, is idempotent, and validates its arguments.
/// </summary>
[TestFixture]
public sealed class AddTelemetryToolsTests
{
    // The tool-group service interface is internal to the MCP package; obtain its
    // Type via the accessible TelemetryToolGroup rather than naming it.
    private static readonly Type ToolGroupInterface = typeof(TelemetryToolGroup)
        .GetInterfaces()
        .Single(i => i.Name == "ILatticeApiMcpToolGroup");

    private static void ConfigureValid(LatticeApiMcpTelemetryOptions options)
        => options.BackendAddress = new Uri("https://prometheus.internal:9090/");

    [Test]
    public void AddTelemetryTools_registers_a_single_telemetry_tool_group()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices(ToolGroupInterface).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Exactly(1).InstanceOf<TelemetryToolGroup>());
            Assert.That(((TelemetryToolGroup)groups.Single()!).Group,
                Is.EqualTo(LatticeApiMcpGroup.Telemetry));
        });
    }

    [Test]
    public void AddTelemetryTools_is_idempotent_for_the_tool_group()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(ConfigureValid);
        services.AddTelemetryTools(ConfigureValid);

        var registrations = services.Count(d => d.ServiceType == ToolGroupInterface);
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddTelemetryTools_binds_the_options()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.RequestTimeout = TimeSpan.FromSeconds(7);
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.BackendAddress, Is.EqualTo(new Uri("https://prometheus.internal:9090/")));
            Assert.That(options.RequestTimeout, Is.EqualTo(TimeSpan.FromSeconds(7)));
        });
    }

    [Test]
    public void AddTelemetryTools_registers_the_options_validator()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(_ => { }); // Deliberately leaves the backend address unset.

        using var provider = services.BuildServiceProvider();
        var validators = provider.GetServices<IValidateOptions<LatticeApiMcpTelemetryOptions>>().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(validators, Has.Exactly(1).InstanceOf<LatticeApiMcpTelemetryOptionsValidator>());
            Assert.Throws<OptionsValidationException>(
                () => _ = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value);
        });
    }

    [Test]
    public void AddTelemetryTools_registers_the_default_backend_client()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IPrometheusQueryClient>();

        Assert.That(client, Is.InstanceOf<PrometheusQueryClient>());
    }

    [Test]
    public void AddTelemetryTools_defers_to_a_host_supplied_backend_client()
    {
        var custom = Substitute.For<IPrometheusQueryClient>();
        var services = new ServiceCollection();
        services.AddSingleton(custom);
        services.AddTelemetryTools(ConfigureValid);

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IPrometheusQueryClient>(), Is.SameAs(custom));
    }

    [Test]
    public void AddTelemetryTools_registers_the_metric_access_policy()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
            o.AllowedMetrics.Add("lattice_wal_*");
        });

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
    public void AddTelemetryTools_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => ((IServiceCollection)null!).AddTelemetryTools(ConfigureValid));

    [Test]
    public void AddTelemetryTools_rejects_a_null_configure_delegate()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddTelemetryTools(configure: null!));

    [Test]
    public void AddTelemetryTools_points_the_backend_http_client_at_the_configured_address_and_timeout()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.RequestTimeout = TimeSpan.FromSeconds(7);
        });

        using var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IHttpClientFactory>().CreateClient(BackendClientName);

        Assert.Multiple(() =>
        {
            Assert.That(client.BaseAddress, Is.EqualTo(new Uri("https://prometheus.internal:9090/")));
            Assert.That(client.Timeout, Is.EqualTo(TimeSpan.FromSeconds(7)));
        });
    }

    [Test]
    public void AddTelemetryTools_presents_the_configured_client_certificate_on_the_transport()
    {
        // Mutual TLS is the one auth mode whose credential is not a request header:
        // the certificate must reach the primary transport handler, or the backend
        // would see an anonymous connection.
        using var certificate = SelfSignedCertificate();
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.AuthMode = LatticeTelemetryBackendAuthMode.MutualTls;
            o.Credential = new LatticeTelemetryBackendCredential { ClientCertificate = certificate };
        });

        using var provider = services.BuildServiceProvider();
        var handler = provider.GetRequiredService<IHttpMessageHandlerFactory>()
            .CreateHandler(BackendClientName);

        var primary = PrimaryHandlerOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(primary, Is.InstanceOf<HttpClientHandler>());
            Assert.That(((HttpClientHandler)primary).ClientCertificates, Does.Contain(certificate));
        });
    }

    [Test]
    public void AddTelemetryTools_leaves_the_transport_anonymous_for_a_header_auth_mode()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
            o.Credential = new LatticeTelemetryBackendCredential { BearerToken = "token" };
        });

        using var provider = services.BuildServiceProvider();
        var handler = provider.GetRequiredService<IHttpMessageHandlerFactory>()
            .CreateHandler(BackendClientName);

        var primary = PrimaryHandlerOf(handler);
        Assert.That(((HttpClientHandler)primary).ClientCertificates, Is.Empty);
    }

    [Test]
    public void AddTelemetryTools_leaves_the_transport_anonymous_when_mutual_tls_has_no_certificate()
    {
        // Defensive: the options validator rejects this pairing, so the handler must
        // simply present nothing rather than fault while building the transport.
        var services = new ServiceCollection();
        services.AddTelemetryTools(o =>
        {
            ConfigureValid(o);
            o.AuthMode = LatticeTelemetryBackendAuthMode.MutualTls;
            o.Credential = new LatticeTelemetryBackendCredential();
        });

        using var provider = services.BuildServiceProvider();

        // Options validation guards the misconfiguration before the transport is
        // ever built, which is the behaviour a host depends on.
        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IHttpMessageHandlerFactory>().CreateHandler(BackendClientName));
    }

    [Test]
    public void AddTelemetryTools_fails_the_host_when_no_backend_address_is_supplied()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(_ => { });

        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IPrometheusQueryClient>());
        Assert.That(
            ex!.Failures,
            Has.Some.Contains(nameof(LatticeApiMcpTelemetryOptions.BackendAddress)));
    }

    [Test]
    public void AddTelemetryTools_registers_a_single_options_validator_when_called_twice()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(ConfigureValid);
        services.AddTelemetryTools(ConfigureValid);

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetServices<IValidateOptions<LatticeApiMcpTelemetryOptions>>()
                .OfType<LatticeApiMcpTelemetryOptionsValidator>(),
            Has.Exactly(1).Items);
    }

    // The typed-client registration names its HttpClient after the client
    // interface, which is how the factory addresses its configuration.
    private const string BackendClientName = nameof(IPrometheusQueryClient);

    private static HttpMessageHandler PrimaryHandlerOf(HttpMessageHandler handler)
    {
        var current = handler;
        while (current is DelegatingHandler delegating && delegating.InnerHandler is { } inner)
        {
            current = inner;
        }

        return current;
    }

    private static X509Certificate2 SelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=lattice-telemetry-registration-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
    }
}
