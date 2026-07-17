using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for the <see cref="ILatticeIdentityDirectory"/> registration in
/// <see cref="LatticeMembershipServiceCollectionExtensions.AddLatticeMembership(ISiloBuilder, Action{LatticeMembershipOptions})"/>.
/// The silo builder is stubbed over a real service collection so registration is
/// exercised without deploying a cluster.
/// </summary>
public class IdentityDirectoryRegistrationTests
{
    private static (ISiloBuilder Builder, IServiceCollection Services) CreateBuilder()
    {
        var services = new ServiceCollection();

        // AddLatticeMembership's ordering guard keys off the core options
        // validator that AddLattice registers; stub it so the guard passes.
        services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());

        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return (builder, services);
    }

    [Test]
    public void AddLatticeMembership_registers_null_identity_directory_by_default()
    {
        var (builder, services) = CreateBuilder();

        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetService<ILatticeIdentityDirectory>();

        Assert.That(directory, Is.TypeOf<NullIdentityDirectory>());
    }

    [Test]
    public async Task Default_identity_directory_returns_documented_no_op_behaviour()
    {
        var (builder, services) = CreateBuilder();
        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetRequiredService<ILatticeIdentityDirectory>();

        var page = await directory.SearchAsync(new DirectorySearchQuery("q"));
        var resolved = await directory.ResolveAsync("someone");

        Assert.That(directory.ProviderId, Is.EqualTo(NullIdentityDirectory.NullProviderId));
        Assert.That(directory.DescribeEntry(null), Does.Contain("accepted without validation"));
        Assert.That(page.Principals, Is.Empty);
        Assert.That(resolved, Is.Null);
    }

    [Test]
    public void A_real_provider_overrides_the_null_default_last_wins()
    {
        var (builder, services) = CreateBuilder();
        builder.AddLatticeMembership();

        // A real provider registers last-wins with a plain AddSingleton.
        services.AddSingleton<ILatticeIdentityDirectory, FakeIdentityDirectory>();

        using var provider = services.BuildServiceProvider();
        var directory = provider.GetRequiredService<ILatticeIdentityDirectory>();

        Assert.That(directory, Is.TypeOf<FakeIdentityDirectory>());
    }

    [Test]
    public void AddLatticeMembership_registers_identity_directory_options_validator()
    {
        var (builder, services) = CreateBuilder();

        builder.AddLatticeMembership();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetServices<IValidateOptions<LatticeIdentityDirectoryOptions>>()
                .Any(v => v is LatticeIdentityDirectoryOptionsValidator),
            Is.True);
        Assert.That(
            provider.GetRequiredService<IOptions<LatticeIdentityDirectoryOptions>>().Value.DefaultPageSize,
            Is.EqualTo(25));
    }

    private sealed class FakeIdentityDirectory : ILatticeIdentityDirectory
    {
        public string ProviderId => "fake";

        public string DescribeEntry(DirectoryPrincipalKind? kind) => "A fake provider for tests.";

        public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default) =>
            Task.FromResult(DirectorySearchPage.Empty);

        public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default) =>
            Task.FromResult<DirectoryPrincipal?>(null);
    }
}
