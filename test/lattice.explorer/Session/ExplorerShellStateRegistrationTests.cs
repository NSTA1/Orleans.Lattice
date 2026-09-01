using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The shell's state model is registered as one contract: a head cannot end up
/// with the route half and not the preference half.
/// </summary>
/// <remarks>
/// These tests resolve services from a <em>built container</em> rather than
/// constructing them directly, which is the distinction that matters here. The
/// rest of this issue's suite constructs its collaborators by hand, so it proved
/// the types work but never that the registration does - and a registration
/// defect that leaves the preference contract inoperative on both heads is
/// invisible to every hand-constructed test. The container path is a separate
/// surface and needs its own coverage.
/// </remarks>
[TestFixture]
public sealed class ExplorerShellStateRegistrationTests
{
    private static ServiceProvider Build() =>
        new ServiceCollection().AddExplorerSession().BuildServiceProvider();

    [Test]
    public void AddExplorerSession_Null_Throws()
    {
        Assert.That(
            () => ExplorerSessionServiceCollectionExtensions.AddExplorerSession(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerSession_ResolvesACatalogSeededWithTheShellsKeys()
    {
        // The regression this fixture exists for, and the one shape every other
        // test in this issue missed: they all construct the catalog directly, so
        // none of them exercised the container path a real head uses.
        //
        // Registering ExplorerPreferenceCatalog by implementation type let the
        // container pick its constructor, and it prefers the one with the most
        // satisfiable parameters. An IEnumerable<T> is always satisfiable - the
        // container synthesises an empty sequence - so the seed constructor won
        // with zero keys and the shell's declared keys were never registered.
        // Every IExplorerShellPreferences member then threw "not a registered
        // preference key" on a real head while the unit suite stayed green.
        using var provider = Build();
        using var scope = provider.CreateScope();

        var catalog = scope.ServiceProvider.GetRequiredService<IExplorerPreferenceCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(
                catalog.Keys,
                Is.Not.Empty,
                "a catalog resolved from the container must be seeded, not empty");
            Assert.That(
                catalog.Keys,
                Is.EquivalentTo(ExplorerPreferenceKeys.All),
                "the container must yield the shell's declared contract");
        });
    }

    [Test]
    public void AddExplorerSession_TheResolvedContractCanActuallyReadAndWriteTheShellKeys()
    {
        // The user-visible consequence, asserted end to end rather than inferred
        // from the catalog's contents: with an empty catalog every one of these
        // threw, so the bare-'/' restore path was dead on both heads.
        using var provider = Build();
        using var scope = provider.CreateScope();
        var preferences = scope.ServiceProvider.GetRequiredService<IExplorerShellPreferences>();

        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerPreferenceKeys.All)
            {
                Assert.That(
                    () => preferences.GetOrDefault(key, string.Empty),
                    Throws.Nothing,
                    $"reading '{key.Name}' through the resolved contract must work");
            }

            Assert.That(() => preferences.GetRememberedRoute(), Throws.Nothing);
            Assert.That(async () => await preferences.RememberRouteAsync(ExplorerRoute.Home), Throws.Nothing);
            Assert.That(async () => await preferences.ResetAsync(), Throws.Nothing);
        });
    }

    [Test]
    public void AddExplorerSession_RegistersNoTypeWhoseConstructorTheContainerMustGuessBetween()
    {
        // The generalisation of the bug above, so it cannot come back through a
        // different service. When a descriptor names an implementation TYPE, the
        // container picks the constructor; when it has only one public
        // constructor there is nothing to pick and the registration is
        // unambiguous. Anything with more than one must be registered through an
        // explicit factory that names the constructor it wants.
        var services = new ServiceCollection().AddExplorerSession();

        var ambiguous = services
            .Where(static descriptor => descriptor.ImplementationType is not null)
            .Where(static descriptor => descriptor.ImplementationType!.GetConstructors().Length > 1)
            .Select(static descriptor =>
                $"{descriptor.ServiceType.Name} -> {descriptor.ImplementationType!.Name} "
                + $"({descriptor.ImplementationType!.GetConstructors().Length} public constructors)")
            .ToArray();

        Assert.That(
            ambiguous,
            Is.Empty,
            "A service registered by implementation type leaves the constructor choice to the container, which "
            + "takes the one with the most satisfiable parameters - and an IEnumerable<T> parameter is always "
            + "satisfiable, so an ambiguous type can be silently constructed empty. Register it with an explicit "
            + "factory naming the constructor instead.\n"
            + string.Join('\n', ambiguous));
    }

    [Test]
    public void AddExplorerSession_TheGuardDetectsAnAmbiguousRegistration()
    {
        // Battery test for the guard above: prove the detection fires on a
        // planted example, so a green run means something.
        var services = new ServiceCollection();
        services.AddSingleton<IExplorerPreferenceCatalog, ExplorerPreferenceCatalog>();

        var ambiguous = services
            .Where(static descriptor => descriptor.ImplementationType is not null)
            .Where(static descriptor => descriptor.ImplementationType!.GetConstructors().Length > 1)
            .ToArray();

        Assert.That(ambiguous, Is.Not.Empty, "the detection must fire on a type registered by name");
    }

    [Test]
    public void ExplorerPreferenceCatalog_RegisteredByTypeIsConstructedEmpty()
    {
        // Pins the framework behaviour the fix works around, so the comment on
        // AddExplorerSession is verifiable rather than folklore. If a future
        // container release changes constructor selection, this fails and tells
        // whoever is reading that the workaround can be revisited.
        using var provider = new ServiceCollection()
            .AddSingleton<IExplorerPreferenceCatalog, ExplorerPreferenceCatalog>()
            .BuildServiceProvider();

        Assert.That(
            provider.GetRequiredService<IExplorerPreferenceCatalog>().Keys,
            Is.Empty,
            "the container prefers the IEnumerable<T> constructor and synthesises an empty sequence for it");
    }

    [Test]
    public void AddExplorerSession_RegistersTheRouteModel()
    {
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerShellRouter>(),
            Is.InstanceOf<ExplorerShellRouter>());
    }

    [Test]
    public void AddExplorerSession_RegistersThePreferenceContract()
    {
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerShellPreferences>(),
            Is.InstanceOf<ExplorerShellPreferences>());
    }

    [Test]
    public void AddExplorerSession_WithNoIdentitySources_StillResolvesTheContract()
    {
        // A head or a test that registers the stores without a sign-in or a
        // configured connection must still get a working contract.
        using var provider = Build();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerPreferenceScopeProvider>().Current,
            Is.EqualTo(ExplorerPreferenceScopeIdentity.Empty));
    }

    [Test]
    public void AddExplorerSession_TheKeyCatalogIsShared()
    {
        using var provider = Build();
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        // Key declarations are statements about the application, so two sessions
        // must not be able to disagree about what the contract covers.
        Assert.That(
            first.ServiceProvider.GetRequiredService<IExplorerPreferenceCatalog>(),
            Is.SameAs(second.ServiceProvider.GetRequiredService<IExplorerPreferenceCatalog>()));
    }

    [Test]
    public void AddExplorerSession_TheRouterIsPerSession()
    {
        using var provider = Build();
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.That(
            first.ServiceProvider.GetRequiredService<IExplorerShellRouter>(),
            Is.Not.SameAs(second.ServiceProvider.GetRequiredService<IExplorerShellRouter>()));
    }

    [Test]
    public void AddExplorerSession_Twice_RegistersOneOfEach()
    {
        using var provider = new ServiceCollection()
            .AddExplorerSession()
            .AddExplorerSession()
            .BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerShellRouter>().Count(),
            Is.EqualTo(1));
    }

    [Test]
    public void AddExplorerNavigation_Null_Throws()
    {
        Assert.That(
            () => ExplorerNavigationServiceCollectionExtensions.AddExplorerNavigation(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerNavigation_DefersToAnAlreadyRegisteredRouter()
    {
        var custom = new ExplorerShellRouter();
        using var provider = new ServiceCollection()
            .AddScoped<IExplorerShellRouter>(_ => custom)
            .AddExplorerNavigation()
            .BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<IExplorerShellRouter>(), Is.SameAs(custom));
    }
}
