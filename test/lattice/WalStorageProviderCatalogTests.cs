using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalStorageProviderCatalog"/> - the resolver that
/// backs the pinned-placement WAL fan-out. Covers default-key resolution to the
/// baseline provider, named-key resolution to keyed singletons, fail-closed
/// behaviour for unknown keys, and the <see cref="IWalStorageProviderCatalog.Keys"/>
/// set.
/// </summary>
[TestFixture]
public sealed class WalStorageProviderCatalogTests
{
    private static IServiceProvider BuildServices(
        IWalStorageProvider baseline,
        params (string Key, IWalStorageProvider Provider)[] named)
    {
        var services = new ServiceCollection();
        services.AddSingleton(baseline);
        foreach (var (key, provider) in named)
        {
            services.AddKeyedSingleton(key, provider);
            services.AddSingleton(new WalStorageProviderRegistration(key));
        }
        return services.BuildServiceProvider();
    }

    [Test]
    public void TryGet_resolves_default_key_to_baseline_provider()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var sp = BuildServices(baseline);
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        var found = catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out var provider);

        Assert.That(found, Is.True);
        Assert.That(provider, Is.SameAs(baseline));
    }

    [Test]
    public void TryGet_resolves_named_key_to_keyed_provider()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var secondary = Substitute.For<IWalStorageProvider>();
        var sp = BuildServices(baseline, ("secondary", secondary));
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        var found = catalog.TryGet("secondary", out var provider);

        Assert.That(found, Is.True);
        Assert.That(provider, Is.SameAs(secondary));
    }

    [Test]
    public void TryGet_fails_closed_for_unknown_key()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var sp = BuildServices(baseline);
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        var found = catalog.TryGet("ghost", out var provider);

        Assert.That(found, Is.False);
        Assert.That(provider, Is.Null);
    }

    [Test]
    public void Keys_always_includes_default_even_with_no_named_providers()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var sp = BuildServices(baseline);
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        Assert.That(catalog.Keys, Does.Contain(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(catalog.Keys, Has.Count.EqualTo(1));
    }

    [Test]
    public void Keys_includes_default_and_every_registered_named_key()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var a = Substitute.For<IWalStorageProvider>();
        var b = Substitute.For<IWalStorageProvider>();
        var sp = BuildServices(baseline, ("acct-a", a), ("acct-b", b));
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        Assert.That(catalog.Keys, Is.EquivalentTo(new[]
        {
            IWalStorageProviderCatalog.DefaultProviderKey, "acct-a", "acct-b",
        }));
    }

    [Test]
    public void Keys_deduplicates_repeated_registrations_of_the_same_key()
    {
        var baseline = Substitute.For<IWalStorageProvider>();
        var provider = Substitute.For<IWalStorageProvider>();
        var services = new ServiceCollection();
        services.AddSingleton(baseline);
        services.AddKeyedSingleton<IWalStorageProvider>("dupe", provider);
        services.AddSingleton(new WalStorageProviderRegistration("dupe"));
        services.AddSingleton(new WalStorageProviderRegistration("dupe"));
        var sp = services.BuildServiceProvider();
        var catalog = new WalStorageProviderCatalog(sp, sp.GetServices<WalStorageProviderRegistration>());

        Assert.That(catalog.Keys, Is.EquivalentTo(new[]
        {
            IWalStorageProviderCatalog.DefaultProviderKey, "dupe",
        }));
    }
}
