using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests the one seam the MCP binding adds on top of the transport-neutral
/// telemetry package: the binding's own <see cref="LatticeApiMcpTelemetryOptions"/>
/// is what a host configures, while the neutral proxy, policy, and guardrails bind
/// to <c>IOptions&lt;LatticeTelemetryOptions&gt;</c>. The forwarding must resolve
/// to the very same instance, or the host would configure one object and the
/// backend would read another - silently reverting to defaults, including the
/// read-all metric-access posture.
/// </summary>
[TestFixture]
public sealed class TelemetryOptionsForwardingTests
{
    private static ServiceProvider Provider(Action<LatticeApiMcpTelemetryOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(options =>
        {
            options.BackendAddress = new Uri("https://prometheus.internal:9090/");
            configure(options);
        });

        return services.BuildServiceProvider();
    }

    [Test]
    public void The_neutral_options_resolve_to_the_very_same_instance_the_host_configured()
    {
        using var provider = Provider(_ => { });

        var binding = provider.GetRequiredService<IOptions<LatticeApiMcpTelemetryOptions>>().Value;
        var neutral = provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value;

        Assert.That(neutral, Is.SameAs(binding));
    }

    [Test]
    public void A_deny_all_posture_configured_on_the_binding_reaches_the_neutral_policy()
    {
        using var provider = Provider(options =>
        {
            options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
            options.AllowedMetrics.Add("lattice_wal_*");
        });

        var policy = provider.GetRequiredService<TelemetryMetricAccessPolicy>();

        Assert.Multiple(() =>
        {
            Assert.That(policy.IsReadAll, Is.False, "A forwarding gap would silently restore read-all.");
            Assert.That(policy.IsAdmitted("lattice_wal_append_total"), Is.True);
            Assert.That(policy.IsAdmitted("up"), Is.False);
        });
    }

    [Test]
    public void The_binding_validator_still_runs_when_the_neutral_options_are_resolved_first()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(_ => { }); // Deliberately leaves the backend address unset.

        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(
            () => _ = provider.GetRequiredService<IOptions<LatticeTelemetryOptions>>().Value);
    }

    [Test]
    public void The_forwarded_options_are_registered_once_across_repeated_opt_ins()
    {
        var services = new ServiceCollection();
        services.AddTelemetryTools(o => o.BackendAddress = new Uri("https://prometheus.internal:9090/"));
        services.AddTelemetryTools(o => o.BackendAddress = new Uri("https://prometheus.internal:9090/"));

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IOptions<LatticeTelemetryOptions>)),
            Is.EqualTo(1));
    }
}
