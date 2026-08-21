using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Supplementary registration-time unit tests for
/// <see cref="LatticeAuthServiceCollectionExtensions.AddLatticeAuth"/> that do not
/// require a live silo: the membership ordering guard (the core registration is
/// present but membership is not) and the idempotent repeat-call path (a second
/// call layers a supplied configure delegate but performs the structural wiring
/// only once).
/// </summary>
[TestFixture]
public sealed class LatticeAuthServiceCollectionExtensionsGuardTests
{
    [Test]
    public void AddLatticeAuth_without_membership_throws()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());

        Assert.That(
            () => builder.AddLatticeAuth(),
            Throws.InvalidOperationException.With.Message.Contains("AddLatticeMembership"));
    }

    [Test]
    public void AddLatticeAuth_repeat_call_layers_configuration_and_returns_early()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton<AuthRegistrationMarker>();
        builder.Services.AddOptions<LatticeAuthOptions>();

        var result = builder.AddLatticeAuth(o => o.EnableDurableHistoryView = false);

        Assert.That(result, Is.SameAs(builder), "the repeat call returns the same builder");
        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeAuthOptions>>()
            .Value;
        Assert.That(options.EnableDurableHistoryView, Is.False, "the supplied configure delegate is still layered on a repeat call");
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class CovSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
