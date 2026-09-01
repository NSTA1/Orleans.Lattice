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
/// Two facts are load-bearing and neither is under this repository's control, so
/// both are measured here rather than assumed. Every addressable shell view must
/// resolve to the shell page, or a deep link is a dead end. And no framework or
/// static-asset path may resolve to it, because an asset request that renders the
/// admin console also comes back carrying a second
/// <c>Content-Security-Policy</c> header - which browsers resolve as the
/// intersection of both policies.
/// </para>
/// <para>
/// The second fact is why every declared template but the bare <c>/</c> begins
/// with a literal segment, and why a contributed area is namespaced under
/// <c>/area/</c>. A root catch-all matched everything, including
/// <c>_framework/**</c>, and additionally could not be rebased under a base-path
/// mount at all.
/// </para>
/// <para>
/// The router is rendered with a <c>Found</c> fragment that records the matched
/// page type and renders nothing, so resolution is observed without standing up
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
    [TestCase("/area/tenants")]
    [TestCase("/area/tenants/detail/acme/quotas")]
    [TestCase("/explore/trees/orders/data?tenant=acme")]
    public void Every_addressable_view_resolves_to_the_shell_page(string address)
    {
        Assert.That(Resolve(address), Is.EqualTo(typeof(Home)));
    }

    [Test]
    public void An_unrecognised_address_under_the_area_namespace_still_lands_on_the_shell()
    {
        // A stale bookmark for an area that no longer exists degrades into a
        // shell that can explain itself, which is what the graceful-degradation
        // requirement asks for.
        Assert.That(Resolve("/area/no-such-area"), Is.EqualTo(typeof(Home)));
    }

    [TestCase("/_framework/blazor.web.js")]
    [TestCase("/_content/Orleans.Lattice.Explorer.UI/lattice-shell.css")]
    [TestCase("/favicon.ico")]
    [TestCase("/_blazor")]
    public void An_asset_path_is_never_routed_to_the_shell(string address)
    {
        // The regression this fixture exists for. A root catch-all - or any
        // template with a parameter in its first segment - matches these, so an
        // asset request renders the whole admin console at an asset URL and comes
        // back carrying a second Content-Security-Policy header. Literal-first
        // templates make that impossible rather than merely unlikely.
        Assert.That(
            Resolves(address),
            Is.False,
            $"'{address}' must not be claimed by a shell route");
    }

    [Test]
    public void The_reset_escape_wins_over_the_shell_routes()
    {
        Assert.That(Resolve("/reset-view"), Is.EqualTo(typeof(ResetView)));
    }

    [Test]
    public void The_not_found_page_wins_over_the_shell_routes()
    {
        Assert.That(Resolve("/not-found"), Is.EqualTo(typeof(NotFound)));
    }

    private Type Resolve(string address)
    {
        Assert.That(Resolves(address), Is.True, $"'{address}' did not match any route");
        return _matched[^1];
    }

    private bool Resolves(string address)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        Render<Router>(parameters => parameters
            .Add(router => router.AppAssembly, typeof(Home).Assembly)
            .Add(router => router.Found, Record));

        var before = _matched.Count;
        Services.GetRequiredService<NavigationManager>().NavigateTo(address);
        return _matched.Count > before;
    }

    private RenderFragment Record(RouteData routeData)
    {
        _matched.Add(routeData.PageType);
        return static _ => { };
    }
}
