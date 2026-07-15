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
    public void AddTelemetryTools_rejects_a_null_service_collection()
        => Assert.Throws<ArgumentNullException>(
            () => ((IServiceCollection)null!).AddTelemetryTools(ConfigureValid));

    [Test]
    public void AddTelemetryTools_rejects_a_null_configure_delegate()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddTelemetryTools(configure: null!));
}
