using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A minimal <see cref="ISiloBuilder"/> backed by a plain service collection, used
/// by the schema registration-extension unit tests to assert wiring without
/// standing up a silo. <see cref="WithCoreLattice"/> satisfies the add-on ordering
/// guard (the presence of an <c>IValidateOptions&lt;LatticeOptions&gt;</c> registration,
/// which <c>AddLattice</c> installs) plus the grain-factory the reserved-tree
/// stores depend on.
/// </summary>
internal sealed class FakeSiloBuilder : ISiloBuilder
{
    public IServiceCollection Services { get; } = new ServiceCollection();

    public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();

    /// <summary>
    /// Registers the minimal core surface the schema add-ons require to be present:
    /// the options validator the ordering guard probes for and a grain factory the
    /// stores resolve.
    /// </summary>
    public FakeSiloBuilder WithCoreLattice()
    {
        Services.AddSingleton<IValidateOptions<LatticeOptions>>(
            Substitute.For<IValidateOptions<LatticeOptions>>());
        Services.AddSingleton(Substitute.For<IGrainFactory>());
        return this;
    }
}
