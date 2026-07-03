using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Registration-front-door tests for
/// <see cref="LatticeApiAuthServiceCollectionExtensions.AddLatticeAuthApi"/> that
/// exercise the ordering guard, idempotency, options resolution, and the opt-in
/// (absent-by-default) posture without standing up a full Orleans cluster.
/// </summary>
[TestFixture]
public class LatticeApiAuthRegistrationTests
{
    [Test]
    public void AddLatticeAuthApi_without_AddLatticeAuth_throws()
    {
        var builder = new TestSiloBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddLatticeAuthApi());
        Assert.That(ex!.Message, Does.Contain("AddLatticeAuth"));
    }

    [Test]
    public void AddLatticeAuthApi_after_AddLatticeAuth_binds_options()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLatticeAuth(builder);

        builder.AddLatticeAuthApi(o => o.MaxExplanationRules = 42);

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiAuthOptions>>();
        Assert.That(options.Value.MaxExplanationRules, Is.EqualTo(42));
    }

    [Test]
    public void AddLatticeAuthApi_registers_the_facade()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLatticeAuth(builder);

        builder.AddLatticeAuthApi();

        Assert.That(IsRegistered<ILatticeAuthAdmin>(builder), Is.True);
    }

    [Test]
    public void AddLatticeAuthApi_called_twice_does_not_double_register_marker()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLatticeAuth(builder);

        builder.AddLatticeAuthApi();
        builder.AddLatticeAuthApi();

        var markerRegistrations = builder.Services.Count(
            d => d.ServiceType == typeof(LatticeApiAuthServiceCollectionExtensions.LatticeApiAuthMarker));
        Assert.That(markerRegistrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeAuthApi_returns_same_builder_for_chaining()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLatticeAuth(builder);

        var returned = builder.AddLatticeAuthApi();
        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void Without_AddLatticeAuthApi_no_facade_services_are_registered()
    {
        var builder = new TestSiloBuilder();
        SimulateAddLatticeAuth(builder);

        // The authorization package is present but the auth API add-on is NOT
        // invoked: the facade must not leak into the container, proving the add-on
        // is opt-in and absent by default.
        Assert.That(IsRegistered<ILatticeAuthAdmin>(builder), Is.False);
    }

    private static bool IsRegistered<TService>(TestSiloBuilder builder)
        => builder.Services.Any(d => d.ServiceType == typeof(TService));

    /// <summary>
    /// Mirrors the single registration <c>AddLatticeAuth</c> makes that the auth
    /// API ordering guard probes for, so these unit tests do not need a silo.
    /// </summary>
    private static void SimulateAddLatticeAuth(TestSiloBuilder builder) =>
        builder.Services.AddSingleton(Substitute.For<ILatticeAuthorizationPolicyStore>());

    private sealed class TestSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
