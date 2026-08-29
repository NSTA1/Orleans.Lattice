using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Interaction tests for the adaptive navigation: what actually happens when a
/// caller clicks a destination, opens the drawer, or works the compact overflow
/// menu, at each breakpoint.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveNavInteractionTests
{
    private static readonly LatticeNavItem[] SixDestinations =
    [
        new("explore", "Explore"),
        new("backups", "Backups"),
        new("access", "Access") { IsEnabled = false },
        new("schema", "Schema"),
        new("tenants", "Tenants"),
        new("my-tenant", "My tenant"),
    ];

    private static Func<DesignSystemInteractiveHarness.RenderedElement, bool> ButtonWithText(string text) =>
        element => element.Name == "button" && element.Text == text;

    [TestCase(LatticeBreakpoint.Expanded)]
    [TestCase(LatticeBreakpoint.Medium)]
    public async Task Click_anEnabledDestination_raisesSelectWithItsId(LatticeBreakpoint breakpoint)
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)breakpoint,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add),
            });

        await harness.ClickAsync(ButtonWithText("Backups"));

        Assert.That(selected, Is.EqualTo(new[] { "backups" }));
    }

    [Test]
    public async Task Click_aDisabledDestination_raisesNothing()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add),
            });

        await harness.ClickAsync(ButtonWithText("Access"));

        Assert.That(selected, Is.Empty);
    }

    [Test]
    public async Task Click_aDestinationWithNoHandlerAttached_doesNotThrow()
    {
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
            });

        Assert.That(async () => await harness.ClickAsync(ButtonWithText("Explore")), Throws.Nothing);
    }

    [Test]
    public async Task Click_theCompactBar_raisesSelectWithItsId()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add),
            });

        await harness.ClickAsync(ButtonWithText("Backups"));

        Assert.That(selected, Is.EqualTo(new[] { "backups" }));
    }

    [Test]
    public async Task Click_theDrawerToggle_opensAndClosesTheDrawer()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["DrawerToggleLabel"] = "Menu",
                ["IsDrawerOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.ClickAsync(ButtonWithText("Menu"));
        Assert.That(states, Is.EqualTo(new[] { true }));
        Assert.That(harness.Element(e => e.HasClass("lx-nav-drawer")).Name, Is.EqualTo("nav"));

        await harness.ClickAsync(ButtonWithText("Menu"));
        Assert.That(states, Is.EqualTo(new[] { true, false }));
    }

    [Test]
    public async Task Click_theScrim_closesTheDrawer()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["IsDrawerOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.ClickAsync(e => e.HasClass("lx-nav-scrim"));

        Assert.That(states, Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task Click_theDrawerCloseControl_closesTheDrawer()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["IsDrawerOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.ClickAsync(e => e.HasClass("lx-nav-drawer-close"));

        Assert.That(states, Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task KeyDown_escapeInsideTheDrawer_closesIt()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["IsDrawerOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.KeyDownAsync(e => e.HasClass("lx-nav-drawer"), "Escape");

        Assert.That(states, Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task KeyDown_anyOtherKeyInsideTheDrawer_leavesItOpen()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["IsDrawerOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.KeyDownAsync(e => e.HasClass("lx-nav-drawer"), "Enter");

        Assert.That(states, Is.Empty);
    }

    [Test]
    public async Task Click_theOverflowToggle_opensTheMenu()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.ClickAsync(e => e.HasClass("lx-nav-overflow-toggle"));

        Assert.Multiple(() =>
        {
            Assert.That(states, Is.EqualTo(new[] { true }));
            Assert.That(harness.Elements().Count(e => e.HasClass("lx-nav-overflow-item")),
                Is.EqualTo(SixDestinations.Length));
        });
    }

    [Test]
    public async Task Click_anOverflowDestination_selectsItAndClosesTheMenu()
    {
        var selected = new List<string>();
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add),
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), overflowStates.Add),
            });

        await harness.ClickAsync(e => e.HasClass("lx-nav-overflow-item") && e.Text == "My tenant");

        Assert.Multiple(() =>
        {
            Assert.That(selected, Is.EqualTo(new[] { "my-tenant" }));
            Assert.That(overflowStates, Is.EqualTo(new[] { false }));
        });
    }

    [Test]
    public async Task Click_aDisabledOverflowDestination_selectsNothingAndLeavesTheMenuOpen()
    {
        var selected = new List<string>();
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add),
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), overflowStates.Add),
            });

        await harness.ClickAsync(e => e.HasClass("lx-nav-overflow-item") && e.Text == "Access");

        Assert.Multiple(() =>
        {
            Assert.That(selected, Is.Empty);
            Assert.That(overflowStates, Is.Empty);
        });
    }

    [Test]
    public async Task Render_anOrphanedOverflowIsClosedAndTheBoundCallerIsTold()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        Assert.Multiple(() =>
        {
            Assert.That(states, Is.EqualTo(new[] { false }),
                "a menu the wider shape renders no control for must not be left open silently");
            Assert.That(harness.Elements().Any(e => e.Attribute("role") == "menu"), Is.False);
        });
    }

    [Test]
    public async Task KeyDown_escapeInsideTheOverflowMenu_closesIt()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.KeyDownAsync(e => e.HasClass("lx-nav-overflow"), "Escape");

        Assert.That(states, Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task KeyDown_anyOtherKeyInsideTheOverflowMenu_leavesItOpen()
    {
        var states = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), states.Add),
            });

        await harness.KeyDownAsync(e => e.HasClass("lx-nav-overflow"), "ArrowDown");

        Assert.That(states, Is.Empty);
    }
}
