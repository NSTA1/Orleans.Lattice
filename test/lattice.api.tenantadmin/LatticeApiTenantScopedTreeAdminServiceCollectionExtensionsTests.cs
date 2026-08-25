using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for
/// <see cref="LatticeApiTenantScopedTreeAdminServiceCollectionExtensions"/> that do
/// not require a live silo: the two ordering guards (the tenant-scoped facade must
/// follow both wrapped control surfaces it delegates to), the null-argument guard,
/// idempotent re-registration of the control singleton, and builder chaining.
/// </summary>
[TestFixture]
public sealed class LatticeApiTenantScopedTreeAdminServiceCollectionExtensionsTests
{
    [Test]
    public void AddLatticeTenantScopedTreeAdminApi_with_null_builder_throws()
        => Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeTenantScopedTreeAdminApi(),
            Throws.ArgumentNullException);

    [Test]
    public void AddLatticeTenantScopedTreeAdminApi_without_tree_admin_throws()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        Assert.That(() => builder.AddLatticeTenantScopedTreeAdminApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeTenantScopedTreeAdminApi_without_schema_admin_throws()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeTreeAdmin>());

        Assert.That(() => builder.AddLatticeTenantScopedTreeAdminApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeTenantScopedTreeAdminApi_after_both_surfaces_wires_the_control_once()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeTreeAdmin>());
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        builder.AddLatticeTenantScopedTreeAdminApi();
        builder.AddLatticeTenantScopedTreeAdminApi();

        Assert.That(
            builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantScopedTreeAdmin)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeTenantScopedTreeAdminApi_returns_the_same_builder_for_chaining()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<ILatticeTreeAdmin>());
        builder.Services.AddSingleton(Substitute.For<ILatticeSchemaAdmin>());

        Assert.That(builder.AddLatticeTenantScopedTreeAdminApi(), Is.SameAs(builder));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
