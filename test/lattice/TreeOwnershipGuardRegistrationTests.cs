using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;

namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class TreeOwnershipGuardRegistrationTests
{
    [Test]
    public async Task AddLattice_default_guard_is_singleton_and_allows_independent_and_derived_targets()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLattice((_, _) => { });
        builder.AddLattice((_, _) => { });
        using var provider = services.BuildServiceProvider();
        var guard = provider.GetRequiredService<ITreeOwnershipGuard>();

        Assert.That(provider.GetServices<ITreeOwnershipGuard>().Count(), Is.EqualTo(1));
        Assert.That(guard, Is.SameAs(provider.GetRequiredService<ITreeOwnershipGuard>()));
        foreach (var derivedFrom in new string?[] { null, "logical" })
        {
            var result = guard.AuthorizeAliasAsync("logical", "physical", derivedFrom);
            Assert.That(result.IsCompletedSuccessfully, Is.True);
            Assert.That((await result).Allowed, Is.True);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public void AddLattice_preserves_a_provider_registered_before_or_replaced_after(bool after)
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        var guard = Substitute.For<ITreeOwnershipGuard>();
        if (!after)
            services.AddSingleton(guard);
        builder.AddLattice((_, _) => { });
        if (after)
            services.Replace(ServiceDescriptor.Singleton(guard));
        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ITreeOwnershipGuard>(), Is.SameAs(guard));
    }
}
