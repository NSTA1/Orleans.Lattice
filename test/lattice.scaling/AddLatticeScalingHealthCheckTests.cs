using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for
/// <see cref="LatticeScalingServiceCollectionExtensions.AddLatticeScalingHealthCheck(IHealthChecksBuilder, string, HealthStatus?, System.Collections.Generic.IEnumerable{string})"/>:
/// it must guard a null builder, register the check under the default (and a
/// caller-supplied) name, apply tags, and promote the check to a singleton on
/// the underlying container.
/// </summary>
[TestFixture]
public sealed class AddLatticeScalingHealthCheckTests
{
    [Test]
    public void AddLatticeScalingHealthCheck_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeScalingServiceCollectionExtensions.AddLatticeScalingHealthCheck(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeScalingHealthCheck_registers_check_under_default_name()
    {
        using var provider = BuildProviderWithCheck();
        var registry = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        Assert.That(
            registry.Registrations.Select(r => r.Name),
            Has.Member(LatticeScalingHealthCheckOptions.DefaultName));
    }

    [Test]
    public void AddLatticeScalingHealthCheck_honours_caller_supplied_name_and_tags()
    {
        using var provider = BuildProviderWithCheck(b =>
            b.AddLatticeScalingHealthCheck("custom-scale", tags: new[] { "ready" }));
        var registry = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        var registration = registry.Registrations.Single(r => r.Name == "custom-scale");
        Assert.That(registration.Tags, Has.Member("ready"));
    }

    [Test]
    public void AddLatticeScalingHealthCheck_registers_check_as_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeScalingSignal>());
        services.AddSingleton(BuildOptionsMonitor());
        services.AddLogging();
        services.AddHealthChecks().AddLatticeScalingHealthCheck();

        var registration = services.Single(d => d.ServiceType == typeof(LatticeScalingHealthCheck));

        Assert.That(registration.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
    }

    [Test]
    public void AddLatticeScalingHealthCheck_returns_builder_for_chaining()
    {
        var services = new ServiceCollection();
        var builder = services.AddHealthChecks();

        Assert.That(builder.AddLatticeScalingHealthCheck(), Is.SameAs(builder));
    }

    private static ServiceProvider BuildProviderWithCheck(
        Action<IHealthChecksBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeScalingSignal>());
        services.AddSingleton(BuildOptionsMonitor());
        services.AddLogging();
        var builder = services.AddHealthChecks();
        if (configure is null)
        {
            builder.AddLatticeScalingHealthCheck();
        }
        else
        {
            configure(builder);
        }

        return services.BuildServiceProvider();
    }

    private static IOptionsMonitor<LatticeScalingHealthCheckOptions> BuildOptionsMonitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeScalingHealthCheckOptions>>();
        var options = new LatticeScalingHealthCheckOptions();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }
}
