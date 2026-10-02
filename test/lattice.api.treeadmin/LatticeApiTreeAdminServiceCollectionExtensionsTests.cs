using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
    public void AddLatticeTreeAdminApi_serves_the_wal_reclamation_read_from_the_facade_singleton()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        builder.AddLatticeTreeAdminApi();
        builder.AddLatticeTreeAdminApi();

        var descriptor = builder.Services.Single(d => d.ServiceType == typeof(ILatticeWalReclamation));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(descriptor.ImplementationFactory, Is.Not.Null, "resolved from the one LatticeTreeAdmin singleton");
        });
    }

    [Test]
    public void AddLatticeTreeAdminApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        Assert.That(builder.AddLatticeTreeAdminApi(), Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeTreeAdminApi_applies_the_supplied_configure_delegate()
    {
        // The optional configure delegate is the front door's only caller-supplied
        // arm, and an add-on that accepted it and never registered it would look
        // identical from the registration side: the options type still resolves, so
        // nothing throws and nothing is obviously missing - the host's configuration
        // is simply discarded.
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        var applied = 0;

        builder.AddLatticeTreeAdminApi(_ => applied++);

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiTreeAdminOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options, Is.Not.Null);
            Assert.That(applied, Is.EqualTo(1),
                "the supplied configure delegate must reach the options pipeline");
        });
    }

    [Test]
    public void AddLatticeTreeAdminApi_without_a_configure_delegate_still_resolves_options()
    {
        // The accepting counterpart to the arm above: the no-delegate path must leave
        // the options instance resolvable rather than depending on a caller having
        // supplied one.
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());

        builder.AddLatticeTreeAdminApi();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(
            provider.GetRequiredService<IOptions<LatticeApiTreeAdminOptions>>().Value,
            Is.Not.Null);
    }

    [Test]
    public void AddLatticeTreeAdminApi_called_twice_layers_both_configure_delegates()
    {
        // Documented behaviour: the structural wiring is idempotent, but a repeat
        // call still layers its options delegate above the first. Asserting only the
        // singleton count (as the idempotency test above does) cannot tell a layered
        // delegate from a dropped one.
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        var order = new List<string>();

        builder.AddLatticeTreeAdminApi(_ => order.Add("first"));
        builder.AddLatticeTreeAdminApi(_ => order.Add("second"));

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<LatticeApiTreeAdminOptions>>().Value;

        Assert.That(order, Is.EqualTo(new[] { "first", "second" }));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
