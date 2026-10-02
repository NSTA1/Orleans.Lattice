using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Pins the registration of the tenant policy facade: it resolves from the
/// container over its dependencies, its seams are registered once however often the
/// host calls <c>AddLatticeTenantAdminApi</c>, and without the tenancy add-on's live
/// flag it reads the feature as off, fail-closed.
/// </summary>
[TestFixture]
public sealed class LatticeTenantPolicyAdminRegistrationTests
{
    [Test]
    public void The_facade_and_its_seams_are_registered_once_across_repeated_calls()
    {
        var builder = NewBuilder();

        builder.AddLatticeTenantAdminApi();
        builder.AddLatticeTenantAdminApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeTenantPolicyAdmin)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantPolicyDecisionSource)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantMembershipUsage)), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_facade_resolves_and_reads_the_feature_as_off_without_the_tenancy_flag()
    {
        var builder = NewBuilder();
        builder.AddLatticeTenantAdminApi();
        using var provider = builder.Services.BuildServiceProvider();

        var facade = provider.GetRequiredService<ILatticeTenantPolicyAdmin>();
        TenantAccessPosture posture;
        using (LatticeSystemOrigin.Enter())
        {
            posture = await facade.GetPostureAsync("acme");
        }

        Assert.Multiple(() =>
        {
            Assert.That(facade, Is.TypeOf<LatticeTenantPolicyAdmin>());
            Assert.That(provider.GetRequiredService<ITenantPolicyDecisionSource>(), Is.TypeOf<EngineTenantPolicyDecisionSource>());
            Assert.That(provider.GetRequiredService<ITenantMembershipUsage>(), Is.TypeOf<ScopedStoreTenantMembershipUsage>());
            Assert.That(posture.Enabled, Is.False);
        });
    }

    private static FakeSiloBuilder NewBuilder()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            default,
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed"));

        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ITenantRegistry>(registry);
        builder.Services.AddSingleton(Substitute.For<ILatticeAccessGate>());
        builder.Services.AddSingleton(Substitute.For<ILatticeAuthorizationPolicyStore>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        builder.Services.AddOptions<LatticeAuthOptions>();
        return builder;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
