using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Registration and argument-guard tests for the grain-call observation filter.
/// The behavioural (anti-censoring) coverage lives in
/// <see cref="LatticeGrainCallObservationTests"/>, which needs a cluster; these
/// are pure in-process checks and deliberately carry no slow category.
/// </summary>
[TestFixture]
public sealed class LatticeGrainCallObservationFilterTests
{
    [Test]
    public void AddLatticeGrainCallObservation_when_builder_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => ((ISiloBuilder)null!).AddLatticeGrainCallObservation());
    }

    [Test]
    public void AddLatticeGrainCallObservation_when_called_registers_the_observation_filter()
    {
        var services = new ServiceCollection();

        Wrap(services).AddLatticeGrainCallObservation();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IOutgoingGrainCallFilter)
                && d.ImplementationType == typeof(LatticeGrainCallObservationFilter)),
            Is.EqualTo(1));
    }

    /// <summary>
    /// Registering twice must install one filter. A second instance would keep
    /// its own outstanding-call table, so every call would be counted against
    /// two half-populated tables and the reported depth would be systematically
    /// understated - a quiet failure that looks exactly like a healthy cluster.
    /// </summary>
    [Test]
    public void AddLatticeGrainCallObservation_when_called_twice_registers_a_single_filter()
    {
        var services = new ServiceCollection();
        var builder = Wrap(services);

        builder.AddLatticeGrainCallObservation();
        builder.AddLatticeGrainCallObservation();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IOutgoingGrainCallFilter)
                && d.ImplementationType == typeof(LatticeGrainCallObservationFilter)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeGrainCallObservation_returns_the_same_builder()
    {
        var builder = Wrap(new ServiceCollection());

        Assert.That(builder.AddLatticeGrainCallObservation(), Is.SameAs(builder));
    }

    [Test]
    public void Invoke_when_context_is_null_throws()
    {
        var filter = new LatticeGrainCallObservationFilter();

        Assert.ThrowsAsync<ArgumentNullException>(() => filter.Invoke(null!));
    }

    private static ISiloBuilder Wrap(IServiceCollection services) => new SiloBuilderStub(services);

    private sealed class SiloBuilderStub(IServiceCollection services) : ISiloBuilder
    {
        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();

        public IServiceCollection Services { get; } = services;
    }
}
