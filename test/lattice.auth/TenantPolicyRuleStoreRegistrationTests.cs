using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Pins the registration of the internal <see cref="ITenantPolicyRuleStore"/> the
/// tenancy add-on's tenant deletion pipeline resolves: <c>AddLatticeAuth</c> routes
/// it at the registered policy store, and a host that replaced the store with an
/// implementation that cannot purge fails loudly rather than skipping the purge.
/// </summary>
[TestFixture]
public sealed class TenantPolicyRuleStoreRegistrationTests
{
    private static FakeSiloBuilder CreateHost()
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<IGrainFactory>());
        builder.AddLatticeMembership();
        builder.AddLatticeAuth();
        return builder;
    }

    [Test]
    public void AddLatticeAuth_routes_the_tenant_rule_store_at_the_registered_policy_store()
    {
        // Pre-register the shipped store so resolving it does not pull in the whole
        // silo graph; AddLatticeAuth's TryAdd keeps it.
        var grainFactory = Substitute.For<IGrainFactory>();
        var options = new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions());
        var store = new LatticeAuthorizationPolicyStore(
            grainFactory, new AuthInitializer(grainFactory, Substitute.For<IServiceProvider>(), options), options);
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton<ILatticeAuthorizationPolicyStore>(store);
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.AddLatticeMembership();
        builder.AddLatticeAuth();
        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ITenantPolicyRuleStore>(), Is.SameAs(store));
    }

    [Test]
    public void A_replaced_policy_store_without_purge_support_fails_loudly()
    {
        var builder = CreateHost();
        builder.Services.AddSingleton<ILatticeAuthorizationPolicyStore>(new CovPolicyStore());
        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<ITenantPolicyRuleStore>(),
            Throws.InvalidOperationException.With.Message.Contains("tenant-tier rule maintenance"));
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
