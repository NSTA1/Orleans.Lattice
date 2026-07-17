using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiSchemaServiceCollectionExtensions"/> that do
/// not require a live silo: the ordering guard (the control API must follow the
/// schema enforcement registration), the null-argument guard, and idempotent
/// re-registration of the control singleton. Happy-path wiring is covered by the
/// gRPC binding's integration tests.
/// </summary>
[TestFixture]
public sealed class LatticeApiSchemaServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeSchemaApi_before_enforcement_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeSchemaApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeSchemaApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeSchemaApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeSchemaApi_after_enforcement_wires_the_control_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        builder.AddLatticeSchemaApi();
        builder.AddLatticeSchemaApi();

        var controlRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeSchemaControl));
        Assert.That(controlRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeSchemaApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        Assert.That(builder.AddLatticeSchemaApi(), Is.SameAs(builder));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
