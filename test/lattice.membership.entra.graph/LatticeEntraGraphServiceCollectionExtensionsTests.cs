using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Membership.Entra;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeEntraGraphServiceCollectionExtensions"/>. The
/// silo builder is stubbed over a real service collection so registration and the
/// ordering guard are exercised without deploying a cluster or touching Azure.
/// </summary>
public class LatticeEntraGraphServiceCollectionExtensionsTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder(bool entraRegistered)
    {
        var services = new ServiceCollection();
        if (entraRegistered)
        {
            // AddEntraCredentialAuthenticator registers this marker; the Graph
            // ordering guard keys off its presence.
            services.AddSingleton<EntraAuthenticatorRegistrationMarker>();
        }

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    private static void Configure(LatticeEntraGraphOptions options)
    {
        options.TenantId = "11111111-1111-1111-1111-111111111111";
        options.ClientId = "22222222-2222-2222-2222-222222222222";
        options.ClientSecret = "secret";
    }

    [Test]
    public void AddEntraGraphGroupResolver_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddEntraGraphGroupResolver(Configure),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddEntraGraphGroupResolver_null_configure_throws()
    {
        var (builder, _) = CreateBuilder(entraRegistered: true);

        Assert.That(() => builder.AddEntraGraphGroupResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddEntraGraphGroupResolver_before_entra_authenticator_throws()
    {
        var (builder, _) = CreateBuilder(entraRegistered: false);

        Assert.That(
            () => builder.AddEntraGraphGroupResolver(Configure),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddEntraGraphGroupResolver_after_entra_authenticator_registers_resolver()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(Configure);

        Assert.That(
            services.Any(d => d.ServiceType == typeof(IEntraGroupResolver)),
            Is.True);
    }

    [Test]
    public void AddEntraGraphGroupResolver_registers_identity_directory()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(Configure);

        Assert.That(
            services.Any(d => d.ServiceType == typeof(ILatticeIdentityDirectory)),
            Is.True);
    }

    [Test]
    public void AddEntraGraphGroupResolver_invalid_options_throws()
    {
        var (builder, _) = CreateBuilder(entraRegistered: true);

        // TenantId / ClientId / ClientSecret left unset: the validator rejects them.
        Assert.That(
            () => builder.AddEntraGraphGroupResolver(_ => { }),
            Throws.TypeOf<Microsoft.Extensions.Options.OptionsValidationException>());
    }

    [Test]
    public void Not_registered_leaves_no_graph_footprint()
    {
        var (_, services) = CreateBuilder(entraRegistered: true);

        // Without calling AddEntraGraphGroupResolver there is no resolver: zero
        // cost when the Graph add-on is not used.
        Assert.That(services.Any(d => d.ServiceType == typeof(IEntraGroupResolver)), Is.False);
    }
}
