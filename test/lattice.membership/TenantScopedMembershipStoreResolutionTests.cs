using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="TenantScopedMembershipStoreResolution"/>: the
/// tenant-scoped store is the registered directory itself, and resolution fails
/// closed when no directory, or a directory without the tenant-scoped operations,
/// is registered.
/// </summary>
[TestFixture]
public sealed class TenantScopedMembershipStoreResolutionTests
{
    [Test]
    public void GetTenantScopedMembershipStore_null_provider_throws()
    {
        Assert.That(
            () => TenantScopedMembershipStoreResolution.GetTenantScopedMembershipStore(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetTenantScopedMembershipStore_returns_the_registered_default_directory()
    {
        var directory = TenantGroupNestingTests.CreateDirectory(Substitute.For<IGrainFactory>());
        using var provider = new ServiceCollection()
            .AddSingleton<ILatticeMembershipDirectory>(directory)
            .BuildServiceProvider();

        Assert.That(provider.GetTenantScopedMembershipStore(), Is.SameAs(directory));
    }

    [Test]
    public void GetTenantScopedMembershipStore_without_a_directory_throws()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.That(
            () => provider.GetTenantScopedMembershipStore(),
            Throws.InvalidOperationException.With.Message.Contains("AddLatticeMembership"));
    }

    [Test]
    public void GetTenantScopedMembershipStore_with_a_replaced_directory_throws()
    {
        using var provider = new ServiceCollection()
            .AddSingleton<ILatticeMembershipDirectory>(new CountingDirectory(Array.Empty<string>()))
            .BuildServiceProvider();

        Assert.That(
            () => provider.GetTenantScopedMembershipStore(),
            Throws.InvalidOperationException.With.Message.Contains(nameof(CountingDirectory)));
    }
}
