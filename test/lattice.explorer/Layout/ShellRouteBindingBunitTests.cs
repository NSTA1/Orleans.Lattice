using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// The router-to-browser binding: the layout-owned component that feeds the
/// address into the route model, performs the navigations the router asks for,
/// and remembers where the user ended up.
/// </summary>
/// <remarks>
/// It renders nothing, which is the point - it can therefore sit outside every
/// surface the shell swaps, and outlive them. These are pure unit tests: the
/// route model is in-memory, the preference backing store defaults to in-memory,
/// and bUnit's navigation manager records rather than navigates, so nothing here
/// waits on anything.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellRouteBindingBunitTests : BunitContext
{
    [Test]
    public void The_address_the_binding_mounts_on_becomes_the_route()
    {
        Configure();
        Services.GetRequiredService<NavigationManager>().NavigateTo("/explore/trees/orders/data");

        Render<ShellRouteBinding>();

        var router = Services.GetRequiredService<IExplorerShellRouter>();

        Assert.Multiple(() =>
        {
            Assert.That(router.Current.Kind, Is.EqualTo(ExplorerRouteSegments.Trees));
            Assert.That(router.Current.Id, Is.EqualTo("orders"));
            Assert.That(router.Current.Surface, Is.EqualTo("data"));
        });
    }

    [Test]
    public void A_location_change_is_adopted_as_the_route()
    {
        Configure();
        Render<ShellRouteBinding>();

        // Back and Forward are location changes and nothing else, which is why
        // they need no handling of their own.
        Services.GetRequiredService<NavigationManager>().NavigateTo("/area/tenants");

        Assert.That(Services.GetRequiredService<IExplorerShellRouter>().Current.Area, Is.EqualTo("tenants"));
    }

    [Test]
    public void A_navigation_the_router_asks_for_reaches_the_address_bar()
    {
        Configure();
        Render<ShellRouteBinding>();

        var navigation = Services.GetRequiredService<NavigationManager>();
        Services.GetRequiredService<IExplorerShellRouter>()
            .NavigateTo(ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders"));

        Assert.That(navigation.ToBaseRelativePath(navigation.Uri), Is.EqualTo("explore/trees/orders"));
    }

    [Test]
    public async Task Where_the_user_ended_up_is_remembered()
    {
        Configure();
        Render<ShellRouteBinding>();

        var preferences = Services.GetRequiredService<IExplorerShellPreferences>();
        await preferences.EnsureLoadedAsync();

        Services.GetRequiredService<IExplorerShellRouter>()
            .NavigateTo(ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders").WithSurface("data"));

        Assert.Multiple(() =>
        {
            Assert.That(
                preferences.GetOrDefault(ExplorerPreferenceKeys.Selection, string.Empty),
                Is.EqualTo("orders"));
            Assert.That(
                preferences.GetOrDefault(ExplorerPreferenceKeys.DetailSurface, string.Empty),
                Is.EqualTo("data"));
        });
    }

    private void Configure()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
        Services.AddExplorerSession();
    }
}
