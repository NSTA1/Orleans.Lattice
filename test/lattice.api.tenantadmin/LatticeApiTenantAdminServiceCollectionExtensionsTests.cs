using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiTenantAdminServiceCollectionExtensions"/>
/// that do not require a live silo: the ordering guard (the tenant-administration
/// control API must follow the tenancy add-on whose registry it operates on), the
/// null-argument guard, and idempotent re-registration of the control singleton
/// and its seams.
/// </summary>
[TestFixture]
public sealed class LatticeApiTenantAdminServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeTenantAdminApi_before_tenancy_throws()
    {
        var builder = new FakeSiloBuilder();

        Assert.That(() => builder.AddLatticeTenantAdminApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeTenantAdminApi_with_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeTenantAdminApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeTenantAdminApi_after_tenancy_wires_the_control_and_its_seams_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());
        builder.Services.AddSingleton(new TenantAdminAccessAuthorizer(new FixedGate(true)));

        builder.AddLatticeTenantAdminApi();
        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantAdmin)), Is.EqualTo(1));
            Assert.That(builder.Services.Any(d => d.ServiceType == typeof(ITenantAdminClock)), Is.True);
            Assert.That(builder.Services.Any(d => d.ServiceType == typeof(ITenantTreeCascade)), Is.True);
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantSelfService)),
                Is.EqualTo(1),
                "The read-only tenant self-awareness facade is the single tenancy-enabled signal the MCP binding keys off.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantAccessAdmin)),
                Is.EqualTo(1),
                "The tenant access-administration facade is wired exactly once alongside the lifecycle facade.");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer)),
                Is.EqualTo(1),
                "Both tenant-tier facades share the one two-tier authorizer.");
        });
    }

    [Test]
    public void AddLatticeTenantAdminApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());

        Assert.That(builder.AddLatticeTenantAdminApi(), Is.SameAs(builder));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
