using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Pins the tenant directory facade's registration (the <c>AddTenantDirectoryAdmin</c>
/// partial): the facade and its two underlays are registered once, the built-in
/// tenant-tier authorizer is upgraded to the flag-aware, group-aware one, and a host's
/// own authorizer registration is left alone.
/// </summary>
[TestFixture]
public sealed class LatticeTenantDirectoryAdminRegistrationTests
{
    private static FakeSiloBuilder NewBuilder()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());
        builder.Services.AddSingleton<ILatticeAccessGate>(new FixedGate(allow: true));
        return builder;
    }

    [Test]
    public void The_facade_and_its_underlays_are_registered_once_across_repeated_calls()
    {
        var builder = NewBuilder();

        builder.AddLatticeTenantAdminApi();
        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantDirectoryAdmin)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantDirectoryStore)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantGroupRuleCascade)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer)), Is.EqualTo(1));
        });
    }

    [Test]
    public void The_built_in_authorizer_is_upgraded_to_the_flag_aware_one()
    {
        var builder = NewBuilder();
        builder.AddLatticeTenantAdminApi();

        using var provider = builder.Services.BuildServiceProvider();
        var authorizer = provider.GetRequiredService<TenantRegionResidencyAuthorizer>();

        Assert.That(authorizer.IsDelegatedAccessAware, Is.True);
    }

    [Test]
    public void A_host_registered_authorizer_is_left_in_place()
    {
        var builder = NewBuilder();
        var hostAuthorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), new FakeTenantRegistry());
        builder.Services.AddSingleton(hostAuthorizer);

        builder.AddLatticeTenantAdminApi();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<TenantRegionResidencyAuthorizer>(), Is.SameAs(hostAuthorizer));
    }

    [Test]
    public void A_host_override_registered_after_the_built_in_one_survives_a_repeated_call()
    {
        var builder = NewBuilder();
        builder.AddLatticeTenantAdminApi();
        var hostAuthorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), new FakeTenantRegistry());
        builder.Services.AddSingleton(hostAuthorizer);

        builder.AddLatticeTenantAdminApi();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<TenantRegionResidencyAuthorizer>(), Is.SameAs(hostAuthorizer));
    }

    [Test]
    public void A_repeated_call_leaves_the_upgraded_registrations_unchanged()
    {
        var builder = NewBuilder();
        builder.AddLatticeTenantAdminApi();
        var authorizer = builder.Services.Single(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer));
        var accessAdmin = builder.Services.Single(d => d.ServiceType == typeof(ILatticeTenantAccessAdmin));

        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Single(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer)), Is.SameAs(authorizer));
            Assert.That(builder.Services.Single(d => d.ServiceType == typeof(ILatticeTenantAccessAdmin)), Is.SameAs(accessAdmin));
        });
    }

    [Test]
    public void The_built_in_access_admin_is_upgraded_to_verify_tenant_group_admin_entries()
    {
        var builder = NewBuilder();
        builder.Services.AddSingleton<ITenantDirectoryStore>(new DirectoryTestSupport.FakeTenantDirectoryStore());
        builder.AddLatticeTenantAdminApi();

        using var provider = builder.Services.BuildServiceProvider();
        var admin = provider.GetRequiredService<ILatticeTenantAccessAdmin>();

        Assert.That(((LatticeTenantAccessAdmin)admin).VerifiesTenantGroups, Is.True);
    }

    [Test]
    public void DelegatedAccessReader_fails_closed_without_a_tenancy_flag()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.That(LatticeApiTenantAdminServiceCollectionExtensions.DelegatedAccessReader(provider)(), Is.False);
    }

    [Test]
    public void IsBuiltInRegistration_recognises_only_this_class_factories()
    {
        var builder = NewBuilder();
        builder.AddLatticeTenantAdminApi();
        var builtIn = builder.Services.Single(d => d.ServiceType == typeof(TenantRegionResidencyAuthorizer));

        Assert.Multiple(() =>
        {
            Assert.That(LatticeApiTenantAdminServiceCollectionExtensions.IsBuiltInRegistration(builtIn), Is.True);
            Assert.That(
                LatticeApiTenantAdminServiceCollectionExtensions.IsBuiltInRegistration(
                    ServiceDescriptor.Singleton(_ => new TenantRegionResidencyAuthorizer(new FixedGate(true), new FakeTenantRegistry()))),
                Is.False);
            Assert.That(
                LatticeApiTenantAdminServiceCollectionExtensions.IsBuiltInRegistration(
                    ServiceDescriptor.Singleton(new TenantRegionResidencyAuthorizer(new FixedGate(true), new FakeTenantRegistry()))),
                Is.False);
        });
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
