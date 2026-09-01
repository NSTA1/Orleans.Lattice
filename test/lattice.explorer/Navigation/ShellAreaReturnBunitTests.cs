using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// <b>Leaving a contributed area is a round trip.</b> The rail returns to the
/// home area, and browser Back out of <c>/area/{slug}</c> does too.
/// </summary>
/// <remarks>
/// <para>
/// The defect this fixture pins is structural rather than logical, which is why
/// it survived the shell's own unit tests. The router-to-browser binding lived on
/// the routable page, and the app shell stops rendering its child content while a
/// contributed area owns the working surface - which disposes that page. From
/// that moment nothing performed a navigation the router asked for and nothing
/// observed a location change, so clicking Explore re-mounted the page, which
/// immediately re-adopted the stale <c>/area/{slug}</c> address and bounced the
/// route straight back; and Back was never seen at all.
/// </para>
/// <para>
/// The composition here is the layout's: the binding beside the shell, not
/// inside it. That is the fix, and rendering it any other way would not measure
/// it - so the fixture deliberately mirrors <c>MainLayout</c>'s ordering.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellAreaReturnBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_rail_returns_to_the_home_area_from_a_contributed_area()
    {
        var cut = RenderShell();
        var router = Services.GetRequiredService<IExplorerShellRouter>();

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Alpha").Click();
        Assert.That(router.Current.Area, Is.EqualTo("alpha"), "prove the shell left the home area first");

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Explore").Click();

        Assert.Multiple(() =>
        {
            Assert.That(router.Current.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(
                cut.FindAll(".lx-shell-detail"),
                Has.Count.EqualTo(1),
                "and the home surface is showing again, not the area's view");
        });
    }

    [Test]
    public void Back_out_of_a_contributed_area_returns_to_the_home_area()
    {
        var cut = RenderShell();
        var router = Services.GetRequiredService<IExplorerShellRouter>();
        var navigation = Services.GetRequiredService<NavigationManager>();

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Alpha").Click();

        // Browser Back is a location change and nothing else, so that is exactly
        // how it is driven - no history stack to simulate, no timing to wait on.
        navigation.NavigateTo("/explore");

        Assert.Multiple(() =>
        {
            Assert.That(router.Current.Area, Is.EqualTo(ExplorerRouteSegments.Explore));
            Assert.That(cut.FindAll(".lx-shell-detail"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Entering_a_contributed_area_puts_it_in_the_address_bar()
    {
        var cut = RenderShell();
        var navigation = Services.GetRequiredService<NavigationManager>();

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent.Trim() == "Alpha").Click();

        Assert.That(
            navigation.ToBaseRelativePath(navigation.Uri),
            Is.EqualTo("area/alpha"),
            "an area's slug is the last dotted segment of its plugin id, namespaced under /area/");
    }

    private IRenderedComponent<AppShell> RenderShell()
    {
        // The shell page resolves the real selection service, so it is registered
        // before the shared shell services claim the slot.
        Services.AddSingleton<IExplorerSelection>(new FakeExplorerSelection());
        ConfigureShellServices(AreaPlugin("orleans.lattice.alpha", "Alpha", 100));

        var catalog = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "nav");
            builder.AddContent(1, "catalog-surface");
            builder.CloseElement();
        });

        // The page itself, so the fixture measures what the product mounts and
        // unmounts rather than a stand-in that is never disposed.
        var body = (RenderFragment)(builder =>
        {
            builder.OpenComponent<Home>(0);
            builder.CloseComponent();
        });

        return Render<AppShell>(builder =>
        {
            builder.OpenComponent<ShellRouteBinding>(0);
            builder.CloseComponent();

            builder.OpenComponent<CascadingValue<LatticeAdaptiveContext>>(1);
            builder.AddComponentParameter(2, nameof(CascadingValue<LatticeAdaptiveContext>.Value), AdaptiveContext(LatticeBreakpoint.Expanded));
            builder.AddComponentParameter(3, nameof(CascadingValue<LatticeAdaptiveContext>.ChildContent), (RenderFragment)(inner =>
            {
                inner.OpenComponent<AppShell>(0);
                inner.AddComponentParameter(1, nameof(AppShell.Catalog), catalog);
                inner.AddComponentParameter(2, nameof(AppShell.ChildContent), body);
                inner.CloseComponent();
            }));
            builder.CloseComponent();
        });
    }
}
