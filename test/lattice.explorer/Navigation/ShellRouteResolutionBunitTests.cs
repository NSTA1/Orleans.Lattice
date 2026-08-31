using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Routing;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Proves the shell's route templates resolve the way the design assumes: the
/// catch-all makes every addressable view reachable, and the literal routes still
/// win over it.
/// </summary>
/// <remarks>
/// <para>
/// The catch-all is load-bearing twice over. It is what lets
/// <c>/explore/trees/orders/data</c> render the shell at all rather than a dead
/// end, and it is what makes a stale bookmark degrade into an explained fallback
/// instead of a 404. Equally load-bearing is that it does <em>not</em> swallow
/// <c>/reset-view</c>, because the reset escape exists precisely for the user
/// whose remembered state is broken.
/// </para>
/// <para>
/// Both facts rest on Blazor's route precedence rather than on anything this
/// repository controls, so they are measured here rather than assumed. The
/// router is rendered with a <c>Found</c> fragment that records the matched page
/// type and renders nothing, so the resolution is observed without standing up
/// any page's own service graph - this stays a pure unit test.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellRouteResolutionBunitTests : BunitContext
{
    private readonly List<Type> _matched = [];

    [TestCase("/")]
    [TestCase("/explore")]
    [TestCase("/explore/trees")]
    [TestCase("/explore/trees/orders")]
    [TestCase("/explore/trees/orders/data")]
    [TestCase("/explore/trees/t%2Facme%2Forders/data")]
    [TestCase("/tenants")]
    [TestCase("/explore/trees/orders/data?tenant=acme")]
    public void Every_addressable_view_resolves_to_the_shell_page(string address)
    {
        Assert.That(Resolve(address), Is.EqualTo(typeof(Home)));
    }

    [Test]
    public void An_unrecognised_address_still_lands_on_the_shell_rather_than_a_dead_end()
    {
        // A stale bookmark degrades into a shell that can explain itself, which is
        // what the graceful-degradation requirement asks for.
        Assert.That(Resolve("/no-such-area/at-all"), Is.EqualTo(typeof(Home)));
    }

    [Test]
    public void The_reset_escape_wins_over_the_catch_all()
    {
        Assert.That(Resolve("/reset-view"), Is.EqualTo(typeof(ResetView)));
    }

    [Test]
    public void The_not_found_page_wins_over_the_catch_all()
    {
        Assert.That(Resolve("/not-found"), Is.EqualTo(typeof(NotFound)));
    }

    private Type Resolve(string address)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        Render<Router>(parameters => parameters
            .Add(router => router.AppAssembly, typeof(Home).Assembly)
            .Add(router => router.Found, Record));

        Services.GetRequiredService<NavigationManager>().NavigateTo(address);

        return _matched[^1];
    }

    private RenderFragment Record(RouteData routeData)
    {
        _matched.Add(routeData.PageType);
        return static _ => { };
    }
}
