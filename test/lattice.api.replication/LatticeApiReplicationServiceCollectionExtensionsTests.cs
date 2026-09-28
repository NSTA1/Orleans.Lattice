using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiReplicationServiceCollectionExtensions"/>
/// that do not require a live silo: the ordering guard (the control API must
/// follow the replication config authority registration), the null-argument
/// guard, idempotent re-registration, and the layering of the options delegate.
/// </summary>
[TestFixture]
public sealed class LatticeApiReplicationServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeReplicationApi_without_authority_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeReplicationApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeReplicationApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeReplicationApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationApi_after_authority_wires_the_control_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeReplicationConfigAuthority>());
        builder.Services.AddSingleton<ILatticeAccessGate>(new AllowingAccessGate());

        // The facade scopes a caller-supplied tree name through this seam before it
        // authorizes and acts. AddLattice() registers the core no-op resolver in
        // production; this harness stands up only the services it needs, so it
        // supplies the same default-tenant behaviour directly.
        builder.Services.AddSingleton<ITenantContextResolver>(new DefaultTenantContextResolver());

        builder.AddLatticeReplicationApi();
        builder.AddLatticeReplicationApi();

        var controlRegistrations = builder.Services.Count(d => d.ServiceType == typeof(ILatticeReplicationControl));
        Assert.That(controlRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeReplicationApi_resolves_the_control_and_authorizer()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeReplicationConfigAuthority>());
        builder.Services.AddSingleton<ILatticeAccessGate>(new AllowingAccessGate());

        // The facade scopes a caller-supplied tree name through this seam before it
        // authorizes and acts. AddLattice() registers the core no-op resolver in
        // production; this harness stands up only the services it needs, so it
        // supplies the same default-tenant behaviour directly.
        builder.Services.AddSingleton<ITenantContextResolver>(new DefaultTenantContextResolver());

        builder.AddLatticeReplicationApi();

        var control = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeReplicationControl>();
        Assert.That(control, Is.InstanceOf<LatticeReplicationControl>());
    }

    /// <summary>
    /// The <c>configure</c> overload exists solely to populate
    /// <see cref="LatticeApiReplicationOptions"/>, so a registration that accepted
    /// the delegate and dropped it would satisfy every other test in this fixture.
    /// Pins both halves of the contract: registration does not run the delegate
    /// (options binding is lazy), and resolving the options does.
    /// </summary>
    [Test]
    public void AddLatticeReplicationApi_runs_the_configure_delegate_when_the_options_are_resolved()
    {
        var builder = CreateBuilderWithAuthority();
        var invocations = 0;

        builder.AddLatticeReplicationApi(_ => invocations++);

        Assert.That(invocations, Is.Zero, "registration must not eagerly run the configure delegate");

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiReplicationOptions>>().Value;

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
    public void AddLatticeReplicationApi_layers_every_configure_delegate_across_repeat_calls()
    {
        var builder = CreateBuilderWithAuthority();
        var applied = new List<string>();

        builder.AddLatticeReplicationApi(_ => applied.Add("first"));
        builder.AddLatticeReplicationApi(_ => applied.Add("second"));

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<LatticeApiReplicationOptions>>().Value;

        Assert.That(applied, Is.EqualTo(new[] { "first", "second" }));
    }

    /// <summary>
    /// The options instance must resolve even when no delegate is supplied, which
    /// is what the unconditional <c>AddOptions</c> call is for.
    /// </summary>
    [Test]
    public void AddLatticeReplicationApi_without_a_configure_delegate_still_resolves_the_options()
    {
        var builder = CreateBuilderWithAuthority();

        builder.AddLatticeReplicationApi();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IOptions<LatticeApiReplicationOptions>>().Value, Is.Not.Null);
    }

    /// <summary>
    /// The minimum registration set the facade needs: the config authority whose
    /// absence the ordering guard rejects, the core access gate the fail-closed
    /// authorizer resolves, and the tenant resolver <c>AddLattice()</c> supplies in
    /// production.
    /// </summary>
    private static FakeSiloBuilder CreateBuilderWithAuthority()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeReplicationConfigAuthority>());
        builder.Services.AddSingleton<ILatticeAccessGate>(new AllowingAccessGate());
        builder.Services.AddSingleton<ITenantContextResolver>(new DefaultTenantContextResolver());
        return builder;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
