using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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

        builder.AddLatticeReplicationApi();

        var control = builder.Services.BuildServiceProvider().GetRequiredService<ILatticeReplicationControl>();
        Assert.That(control, Is.InstanceOf<LatticeReplicationControl>());
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
