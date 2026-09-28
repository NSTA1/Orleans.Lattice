using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiSchemaServiceCollectionExtensions"/> that do
/// not require a live silo: the ordering guard (the control API must follow the
/// schema enforcement registration), the null-argument guard, idempotent
/// re-registration of the control singleton, and the options delegate the
/// <c>configure</c> overload exists to layer. Happy-path wiring is covered by the
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

    /// <summary>
    /// The <c>configure</c> overload exists solely to populate
    /// <see cref="LatticeApiSchemaOptions"/>, so a registration that accepted the
    /// delegate and dropped it would satisfy every other test in this fixture.
    /// Pins both halves of the contract: registration does not run the delegate
    /// (options binding is lazy), and resolving the options does.
    /// </summary>
    [Test]
    public void AddLatticeSchemaApi_runs_the_configure_delegate_when_the_options_are_resolved()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());
        var invocations = 0;

        builder.AddLatticeSchemaApi(_ => invocations++);

        Assert.That(invocations, Is.Zero, "registration must not eagerly run the configure delegate");

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiSchemaOptions>>().Value;

        Assert.That(options, Is.Not.Null);
        Assert.That(invocations, Is.EqualTo(1));
    }

    /// <summary>
    /// The documented idempotency contract is asymmetric: a repeat call is a no-op
    /// for the structural wiring but still layers its options delegate. The
    /// singleton-count test above pins only the first half, and would pass just as
    /// well if the second call's delegate were discarded.
    /// </summary>
    [Test]
    public void AddLatticeSchemaApi_layers_every_configure_delegate_across_repeat_calls()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());
        var applied = new List<string>();

        builder.AddLatticeSchemaApi(_ => applied.Add("first"));
        builder.AddLatticeSchemaApi(_ => applied.Add("second"));

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<LatticeApiSchemaOptions>>().Value;

        Assert.That(applied, Is.EqualTo(new[] { "first", "second" }));
    }

    /// <summary>
    /// The options instance must resolve even when no delegate is supplied, which
    /// is what the unconditional <c>AddOptions</c> call is for.
    /// </summary>
    [Test]
    public void AddLatticeSchemaApi_without_a_configure_delegate_still_resolves_the_options()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        builder.AddLatticeSchemaApi();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IOptions<LatticeApiSchemaOptions>>().Value, Is.Not.Null);
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
