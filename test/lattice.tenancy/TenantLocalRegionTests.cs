using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantLocalRegion"/>, the single authority for the
/// region id a tenancy silo serves (issue #5575), and for the
/// <see cref="TenantLocalRegionStartupValidator"/> that refuses to start a silo
/// whose region cannot be resolved.
/// </summary>
[TestFixture]
public sealed class TenantLocalRegionTests
{
    private static ILatticeReplicationContext Replication(bool enabled, string replicaId)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.IsReplicationEnabled.Returns(enabled);
        context.LocalReplicaId.Returns(replicaId);
        return context;
    }

    private static TenantLocalRegion Region(string? orleansClusterId, bool replicated, string replicaId = "") =>
        new(Options.Create(new ClusterOptions { ClusterId = orleansClusterId! }), Replication(replicated, replicaId));

    [Test]
    public void Replicated_host_serves_the_replication_cluster_id_not_the_orleans_cluster_id()
    {
        var region = Region("estate-west", replicated: true, replicaId: "west");

        Assert.Multiple(() =>
        {
            Assert.That(region.IsResolved, Is.True);
            Assert.That(region.RegionId, Is.EqualTo("west"));
        });
    }

    [Test]
    public void Single_cluster_host_serves_the_orleans_cluster_id()
    {
        var region = Region("solo", replicated: false);

        Assert.That(region.RegionId, Is.EqualTo("solo"));
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Replicated_host_with_an_empty_replication_cluster_id_fails_closed(string replicaId)
    {
        var region = Region("estate-west", replicated: true, replicaId: replicaId);

        Assert.Multiple(() =>
        {
            Assert.That(region.IsResolved, Is.False, "the Orleans cluster id must never stand in for the region");
            Assert.That(() => region.RegionId,
                Throws.InvalidOperationException.With.Message.Contains("Replication:ClusterId"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void Single_cluster_host_with_an_empty_cluster_id_fails_closed_without_a_default_region(string? clusterId)
    {
        var region = Region(clusterId, replicated: false);

        Assert.Multiple(() =>
        {
            Assert.That(region.IsResolved, Is.False);
            Assert.That(() => region.RegionId,
                Throws.InvalidOperationException.With.Message.Contains("ClusterOptions.ClusterId"));
        });
    }

    [Test]
    public void Ctor_null_cluster_options_throws() =>
        Assert.That(() => new TenantLocalRegion(null!, Replication(false, string.Empty)), Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_replication_context_throws() =>
        Assert.That(
            () => new TenantLocalRegion(Options.Create(new ClusterOptions()), null!),
            Throws.ArgumentNullException);

    [Test]
    public void ForRegion_fixes_the_region()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantLocalRegion.ForRegion("west").RegionId, Is.EqualTo("west"));
            Assert.That(() => TenantLocalRegion.ForRegion(" "), Throws.ArgumentException);
        });
    }

    [Test]
    public void Startup_validator_refuses_to_start_when_the_region_is_unresolved()
    {
        var validator = new TenantLocalRegionStartupValidator(Region("estate-west", replicated: true));

        Assert.That(
            async () => await validator.StartAsync(CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("cannot resolve this silo's region id"));
    }

    [Test]
    public async Task Startup_validator_starts_and_stops_when_the_region_resolves()
    {
        var validator = new TenantLocalRegionStartupValidator(TenantLocalRegion.ForRegion("west"));

        await validator.StartAsync(CancellationToken.None);
        await validator.StopAsync(CancellationToken.None);
    }

    [Test]
    public void AddLatticeTenancy_resolves_the_region_from_the_replication_cluster_id_and_validates_it_on_start()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        builder.Services.Configure<ClusterOptions>(options => options.ClusterId = "estate-west");
        builder.AddLatticeReplication(options => options.ClusterId = "west");
        builder.AddLatticeTenancy();

        using var services = builder.Services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(services.GetRequiredService<TenantLocalRegion>().RegionId, Is.EqualTo("west"));
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(TenantLocalRegionStartupValidator)),
                Is.EqualTo(1));
        });
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class CovSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
