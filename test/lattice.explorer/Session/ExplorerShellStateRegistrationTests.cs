using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The shell's state model is registered as one contract: a head cannot end up
/// with the route half and not the preference half.
/// </summary>
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
