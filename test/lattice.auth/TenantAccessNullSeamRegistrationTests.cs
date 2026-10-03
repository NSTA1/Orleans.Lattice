using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Pins the delegated tenant access administration null seams (epic #4154) in a
/// host that registers auth and membership but no tenancy: both
/// <see cref="ITenantRuleLayer"/> and <see cref="ITenantGroupClaimFilter"/> resolve
/// to their inactive null defaults, and because they are registered with
/// <c>TryAdd</c> an earlier registration (the shape the tenancy add-on's
/// <c>Replace</c> also produces) wins.
/// </summary>
[TestFixture]
public sealed class TenantAccessNullSeamRegistrationTests
{
    private static FakeSiloBuilder CreateAuthAndMembershipHost()
    {
        var builder = new FakeSiloBuilder();

        // AddLatticeMembership / AddLatticeAuth key their ordering guard off the
        // core options validator AddLattice registers; stub it.
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.AddLatticeMembership();
        builder.AddLatticeAuth();
        return builder;
    }

    [Test]
    public void Both_null_seams_are_present_and_inactive_without_tenancy()
    {
        var builder = CreateAuthAndMembershipHost();
        using var provider = builder.Services.BuildServiceProvider();

        var ruleLayer = provider.GetRequiredService<ITenantRuleLayer>();
        var claimFilter = provider.GetRequiredService<ITenantGroupClaimFilter>();

        Assert.Multiple(() =>
        {
            Assert.That(ruleLayer, Is.TypeOf<NullTenantRuleLayer>());
            Assert.That(ruleLayer.IsActive, Is.False);
            Assert.That(claimFilter.GetType().Name, Is.EqualTo("NullTenantGroupClaimFilter"));
            Assert.That(claimFilter.IsActive, Is.False);
        });
    }

    [Test]
    public void Each_seam_is_registered_exactly_once_as_a_singleton()
    {
        var builder = CreateAuthAndMembershipHost();

        // A repeat call is idempotent and must not stack a second registration.
        builder.AddLatticeMembership();
        builder.AddLatticeAuth();

        var ruleLayers = builder.Services.Where(d => d.ServiceType == typeof(ITenantRuleLayer)).ToList();
        var claimFilters = builder.Services.Where(d => d.ServiceType == typeof(ITenantGroupClaimFilter)).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(ruleLayers, Has.Count.EqualTo(1));
            Assert.That(ruleLayers[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(claimFilters, Has.Count.EqualTo(1));
            Assert.That(claimFilters[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    [Test]
    public void The_null_rule_layer_does_not_displace_an_earlier_registration()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        var active = new ActiveRuleLayer();
        builder.Services.AddSingleton<ITenantRuleLayer>(active);

        builder.AddLatticeMembership();
        builder.AddLatticeAuth();

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantRuleLayer>(), Is.SameAs(active));
    }

    [Test]
    public void A_later_Replace_displaces_the_null_rule_layer()
    {
        var builder = CreateAuthAndMembershipHost();
        var active = new ActiveRuleLayer();
        builder.Services.Replace(ServiceDescriptor.Singleton<ITenantRuleLayer>(active));

        using var provider = builder.Services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ITenantRuleLayer>(), Is.SameAs(active));
    }

    private sealed class ActiveRuleLayer : ITenantRuleLayer
    {
        public bool IsActive => true;
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
