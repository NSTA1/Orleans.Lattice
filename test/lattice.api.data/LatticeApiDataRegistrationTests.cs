using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeApiDataServiceCollectionExtensions.AddLatticeDataApi"/> that
/// exercise the ordering guard, idempotency, options resolution, and the opt-in
/// (absent-by-default) posture without standing up a full Orleans cluster.
/// </summary>
[TestFixture]
public class LatticeApiDataRegistrationTests
{
    [Test]
    public void AddLatticeDataApi_without_AddLattice_throws()
    {
        var builder = new TestSiloBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddLatticeDataApi());
        Assert.That(ex!.Message, Does.Contain("AddLattice"));
    }

    [Test]
    public void AddLatticeDataApi_after_AddLattice_binds_options()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeDataApi(o => o.DefaultRangePageSize = 42);

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiDataOptions>>();
        Assert.That(options.Value.DefaultRangePageSize, Is.EqualTo(42));
    }

    [Test]
    public void AddLatticeDataApi_registers_the_facade()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeDataApi();

        Assert.That(IsRegistered<ILatticeDataApi>(builder), Is.True);
    }

    [Test]
    public void AddLatticeDataApi_called_twice_does_not_double_register_marker()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        builder.AddLatticeDataApi();
        builder.AddLatticeDataApi();

        var markerRegistrations = builder.Services.Count(
            d => d.ServiceType == typeof(LatticeApiDataServiceCollectionExtensions.LatticeApiDataMarker));
        Assert.That(markerRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeDataApi_returns_same_builder_for_chaining()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        var returned = builder.AddLatticeDataApi();
        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void Without_AddLatticeDataApi_no_facade_services_are_registered()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLattice(builder);

        // The core lattice is present but the data API add-on is NOT invoked: the
        // facade must not leak into the container, proving the add-on is opt-in
        // and absent by default.
        Assert.That(IsRegistered<ILatticeDataApi>(builder), Is.False);
    }

    private static bool IsRegistered<TService>(TestSiloBuilder builder)
        => builder.Services.Any(d => d.ServiceType == typeof(TService));

    /// <summary>
    /// Mirrors the single registration <c>AddLattice</c> makes that the data API
    /// ordering guard probes for, so these unit tests do not need a silo.
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
