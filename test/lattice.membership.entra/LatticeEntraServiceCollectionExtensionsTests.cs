using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeEntraServiceCollectionExtensions"/>. The silo
/// builder is stubbed over a real service collection so registration and the
/// ordering guard are exercised without deploying a cluster.
/// </summary>
public class LatticeEntraServiceCollectionExtensionsTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder(bool membershipRegistered)
    {
        var services = new ServiceCollection();
        if (membershipRegistered)
        {
            // AddLatticeMembership registers IValidateOptions<LatticeMembershipOptions>;
            // the Entra ordering guard keys off that.
            services.AddSingleton(Substitute.For<IValidateOptions<LatticeMembershipOptions>>());
        }

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    private static void Configure(LatticeEntraAuthenticatorOptions options)
    {
        options.Authority = "https://login.microsoftonline.com/common/v2.0";
        options.TenantIds.Add("11111111-1111-1111-1111-111111111111");
        options.Audiences.Add("api://lattice");
    }

    [Test]
    public void AddEntraCredentialAuthenticator_null_builder_throws()
    {
        Assert.That(
            () => ((ISiloBuilder)null!).AddEntraCredentialAuthenticator(Configure),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddEntraCredentialAuthenticator_null_configure_throws()
    {
        var (builder, _) = CreateBuilder(membershipRegistered: true);

        Assert.That(() => builder.AddEntraCredentialAuthenticator(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddEntraCredentialAuthenticator_before_membership_throws()
    {
        var (builder, _) = CreateBuilder(membershipRegistered: false);

        Assert.That(
            () => builder.AddEntraCredentialAuthenticator(Configure),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddEntraCredentialAuthenticator_after_membership_registers_authenticator()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);

        builder.AddEntraCredentialAuthenticator(Configure);

        Assert.That(
            services.Any(d => d.ServiceType == typeof(ILatticeCredentialAuthenticator)),
            Is.True);
        Assert.That(
            services.Any(d => d.ServiceType == typeof(EntraAuthenticatorRegistrationMarker)),
            Is.True);
    }

    [Test]
    public void Not_registered_leaves_no_entra_footprint()
    {
        var (_, services) = CreateBuilder(membershipRegistered: true);

        // Without calling AddEntraCredentialAuthenticator there is no authenticator
        // and no marker: zero cost when the add-on is not used.
        Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeCredentialAuthenticator)), Is.False);
        Assert.That(services.Any(d => d.ServiceType == typeof(EntraAuthenticatorRegistrationMarker)), Is.False);
    }

    [Test]
    public void AddEntraCredentialAuthenticator_invalid_options_throws_when_resolved()
    {
        var (builder, services) = CreateBuilder(membershipRegistered: true);
        builder.AddEntraCredentialAuthenticator(options =>
        {
            // Intentionally incomplete: no tenant / audience.
            options.Authority = "https://login.microsoftonline.com/common/v2.0";
        });

        var provider = services.BuildServiceProvider();

        Assert.That(
            () => provider.GetServices<ILatticeCredentialAuthenticator>().ToList(),
            Throws.TypeOf<OptionsValidationException>());
    }
}
