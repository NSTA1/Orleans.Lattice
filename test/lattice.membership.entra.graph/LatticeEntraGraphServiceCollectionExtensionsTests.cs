using Microsoft.Extensions.DependencyInjection;
using Microsoft.Graph;
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

    private static void ConfigureCredential(LatticeEntraGraphOptions options)
        => options.Credential = new FakeTokenCredential();

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
    public void AddEntraGraphGroupResolver_with_credential_registers_resolver()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(ConfigureCredential);

        Assert.That(
            services.Any(d => d.ServiceType == typeof(IEntraGroupResolver)),
            Is.True);
    }

    [Test]
    public void AddEntraGraphGroupResolver_with_credential_registers_identity_directory()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(ConfigureCredential);

        Assert.That(
            services.Any(d => d.ServiceType == typeof(ILatticeIdentityDirectory)),
            Is.True);
    }

    [Test]
    public void AddEntraGraphGroupResolver_with_credential_builds_graph_client()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(ConfigureCredential);

        // Resolving the resolver forces the shared GraphServiceClient factory to
        // run the secret-less branch (built directly from the TokenCredential).
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IEntraGroupResolver>(), Is.Not.Null);
    }

    [Test]
    public void AddEntraGraphGroupResolver_with_client_secret_builds_the_shared_graph_client()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(Configure);

        // Resolving forces the confidential-client branch of the shared
        // GraphServiceClient factory to run: MSAL application build, token
        // acquirer, refresh-skew provider, and authentication provider.
        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetService<IEntraGroupResolver>(), Is.Not.Null);
    }

    [Test]
    public void AddEntraGraphGroupResolver_resolves_the_identity_directory()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(Configure);

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetService<ILatticeIdentityDirectory>();

        Assert.That(directory, Is.InstanceOf<EntraGraphIdentityDirectory>());
    }

    [Test]
    public void AddEntraGraphGroupResolver_with_credential_resolves_the_identity_directory()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(ConfigureCredential);

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetService<ILatticeIdentityDirectory>(),
            Is.InstanceOf<EntraGraphIdentityDirectory>());
    }

    [Test]
    public void The_resolver_and_the_directory_share_one_graph_client()
    {
        // A single token stream no matter how many Graph-backed seams consume
        // it: both registrations must resolve the same singleton.
        var (builder, services) = CreateBuilder(entraRegistered: true);

        builder.AddEntraGraphGroupResolver(Configure);

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IEntraGroupResolver>();
        provider.GetRequiredService<ILatticeIdentityDirectory>();

        Assert.That(
            provider.GetRequiredService<GraphServiceClient>(),
            Is.SameAs(provider.GetRequiredService<GraphServiceClient>()));
    }

    [Test]
    public void The_identity_directory_honours_registered_directory_options()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);
        services.Configure<LatticeIdentityDirectoryOptions>(o => o.MaxPageSize = 7);

        builder.AddEntraGraphGroupResolver(Configure);

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetService<ILatticeIdentityDirectory>(), Is.Not.Null);
    }

    [Test]
    public void A_registered_time_provider_is_used_for_the_token_refresh_clock()
    {
        var (builder, services) = CreateBuilder(entraRegistered: true);
        services.AddSingleton(TimeProvider.System);

        builder.AddEntraGraphGroupResolver(Configure);

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetService<IEntraGroupResolver>(), Is.Not.Null);
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
