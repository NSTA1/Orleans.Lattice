using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for
/// <see cref="LatticeMembershipServiceCollectionExtensions.AddStaticIdentityDirectory(ISiloBuilder, Action{StaticIdentityDirectoryOptions})"/>:
/// the static provider overrides the null default (last-wins) and its roster is
/// applied. The silo builder is stubbed over a real service collection so
/// registration is exercised without deploying a cluster.
/// </summary>
public class StaticIdentityDirectoryRegistrationTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    [Test]
    public void AddStaticIdentityDirectory_overrides_the_null_default_last_wins()
    {
        var (builder, services) = CreateBuilder();
        builder.AddLatticeMembership();

        builder.AddStaticIdentityDirectory(o => o.AddUser("alice"));

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetRequiredService<ILatticeIdentityDirectory>();

        Assert.That(directory, Is.TypeOf<StaticIdentityDirectory>());
        Assert.That(directory.ProviderId, Is.EqualTo("static"));
    }

    [Test]
    public async Task AddStaticIdentityDirectory_applies_the_configured_roster()
    {
        var (builder, services) = CreateBuilder();
        builder.AddLatticeMembership();
        builder.AddStaticIdentityDirectory(o => o.AddUser("alice").AddGroup("admins"));

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetRequiredService<ILatticeIdentityDirectory>();

        Assert.That(await directory.ResolveAsync("alice"), Is.Not.Null);
        Assert.That(await directory.ResolveAsync("admins"), Is.Not.Null);
        Assert.That(await directory.ResolveAsync("nobody"), Is.Null);
    }

    [Test]
    public void AddStaticIdentityDirectory_null_builder_throws()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.AddStaticIdentityDirectory(o => o.AddUser("alice")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddStaticIdentityDirectory_null_configure_throws()
    {
        var (builder, _) = CreateBuilder();

        Assert.That(
            () => builder.AddStaticIdentityDirectory(null!),
            Throws.ArgumentNullException);
    }
}
