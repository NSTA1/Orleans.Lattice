using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeOidcServiceCollectionExtensions"/>. The silo
/// builder is stubbed over a real service collection so registration, the
/// ordering guard, and the configuration-source seam are exercised without
/// deploying a cluster or touching the network.
/// </summary>
public class LatticeOidcServiceCollectionExtensionsTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder(bool membershipRegistered)
    {
        var services = new ServiceCollection();
        if (membershipRegistered)
        {
            // AddLatticeMembership registers IValidateOptions<LatticeMembershipOptions>;
            // the OIDC ordering guard keys off that.
            services.AddSingleton(Substitute.For<IValidateOptions<LatticeMembershipOptions>>());
        }

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    private static void Configure(LatticeOidcAuthenticatorOptions options)
    {
        options.Authority = OidcTestAuthority.Authority;
        options.Issuer = OidcTestAuthority.Issuer;
        options.Audiences.Add(OidcTestAuthority.Audience);
    }

    [Test]
    public void AddLatticeOidc_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddLatticeOidc(Configure),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeOidc_null_configure_throws()
    {
        var (builder, _) = CreateBuilder(membershipRegistered: true);

        Assert.That(() => builder.AddLatticeOidc(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeOidc_before_membership_throws()
    {
        var (builder, _) = CreateBuilder(membershipRegistered: false);

        Assert.That(() => builder.AddLatticeOidc(Configure), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeOidc_after_membership_registers_the_authenticator()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);

        builder.AddLatticeOidc(Configure);

        Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeCredentialAuthenticator)), Is.True);
    }

    [Test]
    public void AddLatticeOidc_returns_the_same_builder()
    {
        var (builder, _) = CreateBuilder(membershipRegistered: true);

        Assert.That(builder.AddLatticeOidc(Configure), Is.SameAs(builder));
    }

    [Test]
    public void AddLatticeOidc_resolves_an_oidc_authenticator()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);
        builder.AddLatticeOidc(Configure);

        using var provider = services.BuildServiceProvider();
        var authenticator = provider.GetRequiredService<ILatticeCredentialAuthenticator>();

        Assert.That(authenticator, Is.InstanceOf<OidcCredentialAuthenticator>());
    }

    [Test]
    public void AddLatticeOidc_prefers_a_container_registered_configuration_source()
    {
        using var authority = new OidcTestAuthority();
        var source = authority.CreateConfigurationSource();
        var (builder, services) = CreateBuilder(membershipRegistered: true);
        services.AddSingleton<IOidcConfigurationSource>(source);
        builder.AddLatticeOidc(Configure);

        using var provider = services.BuildServiceProvider();
        var authenticator = provider.GetRequiredService<ILatticeCredentialAuthenticator>();

        Assert.That(authenticator.CanHandle(new LatticeCredential(authority.MintToken())), Is.True);
    }

    [Test]
    public void AddLatticeOidc_invalid_options_throw_on_resolution()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);
        builder.AddLatticeOidc(o => o.Authority = OidcTestAuthority.Authority);

        using var provider = services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<ILatticeCredentialAuthenticator>(),
            Throws.TypeOf<OptionsValidationException>());
    }

    [Test]
    public void AddLatticeOidc_twice_registers_one_authenticator_per_issuer()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);

        builder.AddLatticeOidc(Configure);
        builder.AddLatticeOidc(o =>
        {
            o.Authority = "https://other-idp.example.com/oauth2/default";
            o.Issuer = OidcTestAuthority.ForeignIssuer;
            o.Audiences.Add("api://other");
        });

        using var provider = services.BuildServiceProvider();
        var authenticators = provider.GetServices<ILatticeCredentialAuthenticator>().ToArray();

        Assert.That(authenticators, Has.Length.EqualTo(2));
        Assert.That(authenticators, Is.All.InstanceOf<OidcCredentialAuthenticator>());
    }

    [Test]
    public void Not_registered_leaves_no_oidc_footprint()
    {
        var (_, services) = CreateBuilder(membershipRegistered: true);

        Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeCredentialAuthenticator)), Is.False);
    }
}
