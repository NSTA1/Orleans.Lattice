using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeApiStateServiceCollectionExtensions.AddLatticeStateApi"/>
/// that exercise the ordering guard, idempotency, and options resolution
/// without standing up a full Orleans cluster.
/// </summary>
[TestFixture]
public class LatticeApiStateRegistrationTests
{
    [Test]
    public void AddLatticeStateApi_without_AddLattice_throws()
    {
        var builder = new TestSiloBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddLatticeStateApi());
        Assert.That(ex!.Message, Does.Contain("AddLattice"));
    }

    [Test]
    public void AddLatticeStateApi_after_AddLattice_binds_options()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeStateApi(o => { });

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiStateOptions>>();
        Assert.That(options.Value, Is.Not.Null);
    }

    [Test]
    public void AddLatticeStateApi_called_twice_does_not_double_register_marker()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeStateApi();
        builder.AddLatticeStateApi();

        var markerRegistrations = builder.Services.Count(
            d => d.ServiceType == typeof(LatticeApiStateServiceCollectionExtensions.LatticeApiStateMarker));
        Assert.That(markerRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeStateApi_returns_same_builder_for_chaining()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        var returned = builder.AddLatticeStateApi();
        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void Without_AddLatticeStateApi_no_facade_services_are_registered()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        // The core lattice is present but the state API add-on is NOT invoked:
        // none of its facade or sampler services must leak into the container,
        // proving the add-on imposes zero structural cost when unregistered.
        Assert.Multiple(() =>
        {
            Assert.That(IsRegistered<ILatticeStateQuery>(builder), Is.False);
            Assert.That(IsRegistered<ILatticeStateObserver>(builder), Is.False);
            Assert.That(IsRegistered<ILatticeStateMetricsObserver>(builder), Is.False);
            Assert.That(IsRegistered<SharedMetricsSampler>(builder), Is.False);
        });
    }

    [Test]
    public void AddLatticeStateApi_registers_the_shared_metrics_sampler_once()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeStateApi();
        builder.AddLatticeStateApi();

        Assert.Multiple(() =>
        {
            Assert.That(IsRegistered<ILatticeStateQuery>(builder), Is.True);
            Assert.That(IsRegistered<ILatticeStateObserver>(builder), Is.True);
            Assert.That(IsRegistered<ILatticeStateMetricsObserver>(builder), Is.True);
            Assert.That(IsRegistered<SharedMetricsSampler>(builder), Is.True);

            // The coalescing sampler is a single shared instance, so repeated
            // registration must not stack duplicate descriptors.
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(SharedMetricsSampler)),
                Is.EqualTo(1));
        });
    }

    private static bool IsRegistered<TService>(TestSiloBuilder builder)
        => builder.Services.Any(d => d.ServiceType == typeof(TService));

    /// <summary>
    /// Mirrors the single registration <c>AddLattice</c> makes that the state
    /// API ordering guard probes for, so these unit tests do not need a silo.
    /// </summary>
    private static void SimulateAddLattice(TestSiloBuilder builder) =>
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>>(
            new ValidateOptions<LatticeOptions>(null, _ => true, "ok"));

    private sealed class TestSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
