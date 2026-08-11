using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiTreeAdminServiceCollectionExtensions"/> that
/// do not require a live silo: the ordering guard (the tree-administration control
/// API must follow the schema control registration it composes), the null-argument
/// guard, and idempotent re-registration of the control singleton.
/// </summary>
[TestFixture]
public sealed class LatticeApiTreeAdminServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeTreeAdminApi_before_schema_api_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeTreeAdminApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeTreeAdminApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeTreeAdminApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeTreeAdminApi_after_schema_api_wires_the_control_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        builder.AddLatticeTreeAdminApi();
        builder.AddLatticeTreeAdminApi();

        var controlRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeTreeAdmin));
        Assert.That(controlRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeTreeAdminApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        Assert.That(builder.AddLatticeTreeAdminApi(), Is.SameAs(builder));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
