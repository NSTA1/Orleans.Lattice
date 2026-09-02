using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// Registration: the appearance feature extends the shell's preference contract
/// rather than replacing it, and does so without either head editing the
/// contract's own key list.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearanceRegistrationTests
{
    [Test]
    public void AddExplorerAppearance_Null_Throws()
    {
        Assert.That(
            () => ExplorerAppearanceServiceCollectionExtensions.AddExplorerAppearance(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerAppearance_RegistersTheState()
    {
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerAppearance>(),
            Is.InstanceOf<ExplorerAppearance>());
    }

    [Test]
    public void AddExplorerAppearance_RegistersTheBrowserApplier()
    {
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerAppearanceApplier>(),
            Is.InstanceOf<ExplorerAppearanceApplier>());
    }

    [Test]
    public void AddExplorerAppearance_DeclaresItsKeysOnTheShellsCatalog()
    {
        // The practical payoff of an enumerated contract: the reset-view page
        // discloses and clears these three with no change to that page.
        using var provider = Build();

        var keys = provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys;

        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(keys, Does.Contain(key), key.Name);
            }
        });
    }

    [Test]
    public void AddExplorerAppearance_KeepsWhateverTheWrappedCatalogAlreadyHeld()
    {
        // Wrapping rather than replacing. Asserted against an explicitly seeded
        // catalog rather than against the one AddExplorerSession() builds,
        // because what the shell seeds its own catalog with is the shell's
        // business and not this feature's to depend on.
        var seeded = new ExplorerPreferenceCatalog(ExplorerPreferenceKeys.All);
        using var provider = Seed()
            .AddSingleton<IExplorerPreferenceCatalog>(seeded)
            .AddExplorerAppearance()
            .BuildServiceProvider();

        var keys = provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys;

        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerPreferenceKeys.All)
            {
                Assert.That(keys, Does.Contain(key), key.Name);
            }

            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(keys, Does.Contain(key), key.Name);
            }
        });
    }

    [Test]
    public void AddExplorerAppearance_CalledTwice_NeitherDuplicatesNorDropsAKey()
    {
        // Two heads composing the same services, or a head that adds it
        // defensively. The wrapper is applied twice; the keys are registered by
        // reference, so re-declaring the same instances must be a no-op.
        var seeded = new ExplorerPreferenceCatalog(ExplorerPreferenceKeys.All);
        using var provider = Seed()
            .AddSingleton<IExplorerPreferenceCatalog>(seeded)
            .AddExplorerAppearance()
            .AddExplorerAppearance()
            .BuildServiceProvider();

        var keys = provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys;

        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerPreferenceKeys.All.Concat(ExplorerAppearancePreferenceKeys.All))
            {
                Assert.That(
                    keys.Count(candidate => ReferenceEquals(candidate, key)),
                    Is.EqualTo(1),
                    key.Name + " must appear exactly once");
            }

            Assert.That(
                keys,
                Has.Count.EqualTo(ExplorerPreferenceKeys.All.Count + ExplorerAppearancePreferenceKeys.All.Count));
        });
    }

    [Test]
    public void AddExplorerAppearance_WrapsAFactoryRegisteredCatalog()
    {
        // The third descriptor shape a head can use. All three must compose,
        // because which one the shell (or a head) happens to use is not this
        // feature's business.
        using var provider = Seed()
            .AddSingleton<IExplorerPreferenceCatalog>(_ => new ExplorerPreferenceCatalog(ExplorerPreferenceKeys.All))
            .AddExplorerAppearance()
            .BuildServiceProvider();

        var keys = provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys;

        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain(ExplorerPreferenceKeys.ActiveArea));
            Assert.That(keys, Does.Contain(ExplorerAppearancePreferenceKeys.Theme));
        });
    }

    [Test]
    public void AddExplorerAppearance_KeepsTheCatalogASingleton()
    {
        // The catalog is where a feature declares a key at composition time, so a
        // wrapper that rebuilt it per resolution would silently drop anything
        // registered onto an earlier copy.
        using var provider = Build();

        Assert.That(
            provider.GetRequiredService<IExplorerPreferenceCatalog>(),
            Is.SameAs(provider.GetRequiredService<IExplorerPreferenceCatalog>()));
    }

    [Test]
    public void AddExplorerAppearance_AfterAddExplorerSession_DeclaresItsKeysOnThatCatalog()
    {
        // The composition order both heads use, and the seam #1850 leans on when
        // it places the control: the appearance keys reach the same catalog the
        // shell's contract reads, without either side editing the other.
        using var provider = Build();
        using var scope = provider.CreateScope();

        var preferences = scope.ServiceProvider.GetRequiredService<IExplorerShellPreferences>();

        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(preferences.Keys, Does.Contain(key), key.Name);
                Assert.That(
                    () => preferences.GetOrDefault(key, string.Empty),
                    Throws.Nothing,
                    key.Name + " must be readable through the contract");
            }
        });
    }

    [Test]
    public void AddExplorerAppearance_PreservesACatalogTheHeadChose()
    {
        // Wrapping rather than replacing: a head that registered its own catalog
        // keeps it, and simply gets three more keys declared on it.
        var chosen = new ExplorerPreferenceCatalog();
        using var provider = Seed()
            .AddSingleton<IExplorerPreferenceCatalog>(chosen)
            .AddExplorerAppearance()
            .BuildServiceProvider();

        var resolved = provider.GetRequiredService<IExplorerPreferenceCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.SameAs(chosen));
            Assert.That(resolved.Keys, Does.Contain(ExplorerAppearancePreferenceKeys.Density));
        });
    }

    [Test]
    public void AddExplorerAppearance_WithoutAHostTheme_StillResolves()
    {
        // The web head's shape: the browser answers prefers-color-scheme in the
        // document, so there is no host opinion to register.
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerAppearance>().Effective.Theme,
            Is.EqualTo(ExplorerThemeChoice.FollowSystem));
    }

    [Test]
    public void AddExplorerAppearance_WithAHostTheme_HonoursIt()
    {
        // The desktop head's shape.
        using var provider = Seed()
            .AddScoped<IExplorerHostTheme>(_ => new FakeExplorerHostTheme(ExplorerHostThemePreference.Light))
            .AddExplorerAppearance()
            .BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerAppearance>().Effective.Theme,
            Is.EqualTo(ExplorerThemeChoice.Light));
    }

    [Test]
    public void AddExplorerAppearance_WithoutTheSessionStores_StillResolvesACatalog()
    {
        var services = new ServiceCollection().AddExplorerAppearance();

        using var provider = services.BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys,
            Does.Contain(ExplorerAppearancePreferenceKeys.Theme));
    }

    private static ServiceProvider Build() => Seed().AddExplorerAppearance().BuildServiceProvider();

    private static IServiceCollection Seed() =>
        new ServiceCollection()
            .AddExplorerSession()
            .AddSingleton<IJSRuntime>(new FakeJsRuntime());
}
