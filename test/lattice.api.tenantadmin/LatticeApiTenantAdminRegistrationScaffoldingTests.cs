using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Pins the registration scaffolding for delegated tenant access administration
/// (epic #4154): <see cref="LatticeApiTenantAdminServiceCollectionExtensions"/> is
/// partial and calls one partial method per facade, so the directory and policy
/// facades each register from their own file. A partial method with no
/// implementation is removed by the compiler together with its call, so the tests
/// read whether each implementation exists by reflection and assert the facade is
/// registered exactly when it does - they stay true before and after each facade
/// lands.
/// </summary>
[TestFixture]
public sealed class LatticeApiTenantAdminRegistrationScaffoldingTests
{
    private const BindingFlags PartialMethodFlags = BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly;

    [Test]
    public void The_registration_extensions_class_stays_a_public_static_class()
    {
        var type = typeof(LatticeApiTenantAdminServiceCollectionExtensions);

        Assert.Multiple(() =>
        {
            Assert.That(type.IsPublic, Is.True);
            Assert.That(type.IsAbstract && type.IsSealed, Is.True, "a static class compiles to abstract sealed");
        });
    }

    [TestCase("AddTenantDirectoryAdmin", typeof(ILatticeTenantDirectoryAdmin))]
    [TestCase("AddTenantPolicyAdmin", typeof(ILatticeTenantPolicyAdmin))]
    public void The_facade_is_registered_exactly_when_its_partial_registration_is_implemented(
        string partialMethodName, Type facadeType)
    {
        var implemented = typeof(LatticeApiTenantAdminServiceCollectionExtensions)
            .GetMethod(partialMethodName, PartialMethodFlags, [typeof(IServiceCollection)]) is not null;
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());

        builder.AddLatticeTenantAdminApi();

        Assert.That(
            builder.Services.Count(d => d.ServiceType == facadeType),
            Is.EqualTo(implemented ? 1 : 0),
            $"{facadeType.Name} must be registered once when {partialMethodName} is implemented, and not at all before.");
    }

    [Test]
    public void The_delegated_facades_are_registered_at_most_once_across_repeated_calls()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(new FakeTenantRegistry());

        builder.AddLatticeTenantAdminApi();
        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantDirectoryAdmin)), Is.LessThanOrEqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantPolicyAdmin)), Is.LessThanOrEqualTo(1));
        });
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
