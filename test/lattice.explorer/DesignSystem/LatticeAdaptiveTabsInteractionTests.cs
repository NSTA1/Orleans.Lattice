using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Interaction tests for the adaptive tab strip: activation by pointer, the
/// WAI-ARIA keyboard pattern (arrow keys, Home, End), and the overflow menu, at
/// each breakpoint.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveTabsInteractionTests
{
    private static readonly LatticeTabItem[] FiveTabs =
    [
        new("metrics", "Metrics"),
        new("topology", "Topology"),
        new("data", "Data") { IsEnabled = false },
        new("history", "History"),
        new("tag-index", "Tag index"),
    ];

    private static Dictionary<string, object?> Parameters(
        LatticeBreakpoint breakpoint,
        string? activeId,
        List<string>? selected = null,
        bool isOverflowOpen = false,
        List<bool>? overflowStates = null,
        IReadOnlyList<LatticeTabItem>? tabs = null)
    {
        var parameters = new Dictionary<string, object?>
        {
            ["Breakpoint"] = (LatticeBreakpoint?)breakpoint,
            ["Tabs"] = tabs ?? FiveTabs,
            ["ActiveId"] = activeId,
            ["IsOverflowOpen"] = isOverflowOpen,
            ["Id"] = "tabs",
        };

        if (selected is not null)
        {
            parameters["OnSelect"] = EventCallback.Factory.Create<string>(new object(), selected.Add);
        }

        if (overflowStates is not null)
        {
            parameters["IsOverflowOpenChanged"] = EventCallback.Factory.Create<bool>(new object(), overflowStates.Add);
        }

        return parameters;
    }

    private static Func<DesignSystemInteractiveHarness.RenderedElement, bool> Tablist =>
        element => element.Attribute("role") == "tablist";

    // ------------------------------------------------------------- activation

    [TestCase(LatticeBreakpoint.Expanded)]
    [TestCase(LatticeBreakpoint.Medium)]
    public async Task Click_anEnabledTab_raisesSelectWithItsId(LatticeBreakpoint breakpoint)
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(breakpoint, "metrics", selected));

        await harness.ClickAsync(e => e.Attribute("role") == "tab" && e.Text == "Topology");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    [Test]
    public async Task Click_aDisabledTab_raisesNothing()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected));

        await harness.ClickAsync(e => e.Attribute("role") == "tab" && e.Text == "Data");

        Assert.That(selected, Is.Empty);
    }

    [Test]
    public async Task Click_aTabWithNoHandlerAttached_doesNotThrow()
    {
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics"));

        Assert.That(
            async () => await harness.ClickAsync(e => e.Attribute("role") == "tab" && e.Text == "Topology"),
            Throws.Nothing);
    }

    // --------------------------------------------------------------- keyboard

    [Test]
    public async Task KeyDown_arrowRight_activatesTheNextTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    [Test]
    public async Task KeyDown_arrowRight_skipsADisabledTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "topology", selected));

        // "data" sits between topology and history and is disabled.
        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.EqualTo(new[] { "history" }));
    }

    [Test]
    public async Task KeyDown_arrowLeft_activatesThePreviousTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "history", selected));

        await harness.KeyDownAsync(Tablist, "ArrowLeft");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    [Test]
    public async Task KeyDown_arrowRight_wrapsPastTheEndOfTheStrip()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "tag-index", selected));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.EqualTo(new[] { "metrics" }));
    }

    [Test]
    public async Task KeyDown_arrowLeft_wrapsPastTheStartOfTheStrip()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected));

        await harness.KeyDownAsync(Tablist, "ArrowLeft");

        Assert.That(selected, Is.EqualTo(new[] { "tag-index" }));
    }

    [Test]
    public async Task KeyDown_home_activatesTheFirstEnabledTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "history", selected));

        await harness.KeyDownAsync(Tablist, "Home");

        Assert.That(selected, Is.EqualTo(new[] { "metrics" }));
    }

    [Test]
    public async Task KeyDown_end_activatesTheLastEnabledTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected));

        await harness.KeyDownAsync(Tablist, "End");

        Assert.That(selected, Is.EqualTo(new[] { "tag-index" }));
    }

    [Test]
    public async Task KeyDown_homeSkipsALeadingDisabledTab()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics") { IsEnabled = false },
            new("topology", "Topology"),
            new("data", "Data"),
        ];

        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "data", selected, tabs: tabs));

        await harness.KeyDownAsync(Tablist, "Home");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    [Test]
    public async Task KeyDown_endSkipsATrailingDisabledTab()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics"),
            new("topology", "Topology"),
            new("data", "Data") { IsEnabled = false },
        ];

        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected, tabs: tabs));

        await harness.KeyDownAsync(Tablist, "End");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    [TestCase("Enter")]
    [TestCase("ArrowUp")]
    [TestCase("PageDown")]
    [TestCase("a")]
    public async Task KeyDown_anUnhandledKey_changesNothing(string key)
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected));

        await harness.KeyDownAsync(Tablist, key);

        Assert.That(selected, Is.Empty);
    }

    [Test]
    public async Task KeyDown_withASingleEnabledTab_doesNotReactivateIt()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics"),
            new("topology", "Topology") { IsEnabled = false },
        ];

        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", selected, tabs: tabs));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.Empty, "moving to the tab already active is a no-op");
    }

    [Test]
    public async Task KeyDown_withEveryTabDisabled_terminatesAndChangesNothing()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics") { IsEnabled = false },
            new("topology", "Topology") { IsEnabled = false },
        ];

        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, null, selected, tabs: tabs));

        await harness.KeyDownAsync(Tablist, "ArrowRight");
        await harness.KeyDownAsync(Tablist, "Home");
        await harness.KeyDownAsync(Tablist, "End");

        Assert.That(selected, Is.Empty);
    }

    [Test]
    public async Task KeyDown_withNoTabs_changesNothing()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, null, selected, tabs: Array.Empty<LatticeTabItem>()));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.Empty);
    }

    [Test]
    public async Task KeyDown_withNothingActive_arrowRightActivatesTheFirstTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, null, selected));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.EqualTo(new[] { "metrics" }));
    }

    [Test]
    public async Task KeyDown_withNothingActive_arrowLeftActivatesTheLastTab()
    {
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, null, selected));

        await harness.KeyDownAsync(Tablist, "ArrowLeft");

        Assert.That(selected, Is.EqualTo(new[] { "tag-index" }));
    }

    [Test]
    public async Task KeyDown_atCompact_stillMovesBetweenTabsThatAreNotInline()
    {
        // Only the active tab is inline at compact, so keyboard navigation is
        // the caller's fastest route through a collapsed strip.
        var selected = new List<string>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", selected));

        await harness.KeyDownAsync(Tablist, "ArrowRight");

        Assert.That(selected, Is.EqualTo(new[] { "topology" }));
    }

    // --------------------------------------------------------------- overflow

    [Test]
    public async Task Click_theOverflowToggle_opensTheMenu()
    {
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", overflowStates: overflowStates));

        await harness.ClickAsync(e => e.HasClass("lx-tabstrip-overflow-toggle"));

        Assert.Multiple(() =>
        {
            Assert.That(overflowStates, Is.EqualTo(new[] { true }));
            Assert.That(
                harness.Elements().Count(e => e.Attribute("role") == "menuitemradio"),
                Is.EqualTo(FiveTabs.Length));
        });
    }

    [Test]
    public async Task Click_anOverflowTab_activatesItAndClosesTheMenu()
    {
        var selected = new List<string>();
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", selected, isOverflowOpen: true, overflowStates));

        await harness.ClickAsync(e => e.HasClass("lx-tabstrip-overflow-item") && e.Text == "Tag index");

        Assert.Multiple(() =>
        {
            Assert.That(selected, Is.EqualTo(new[] { "tag-index" }));
            Assert.That(overflowStates, Is.EqualTo(new[] { false }));
        });
    }

    [Test]
    public async Task Click_aDisabledOverflowTab_activatesNothingAndLeavesTheMenuOpen()
    {
        var selected = new List<string>();
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", selected, isOverflowOpen: true, overflowStates));

        await harness.ClickAsync(e => e.HasClass("lx-tabstrip-overflow-item") && e.Text == "Data");

        Assert.Multiple(() =>
        {
            Assert.That(selected, Is.Empty);
            Assert.That(overflowStates, Is.Empty);
        });
    }

    [Test]
    public async Task Render_anOrphanedOverflowIsClosedAndTheBoundCallerIsTold()
    {
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Expanded, "metrics", isOverflowOpen: true, overflowStates: overflowStates));

        Assert.Multiple(() =>
        {
            Assert.That(overflowStates, Is.EqualTo(new[] { false }),
                "a menu the wider shape renders no control for must not be left open silently");
            Assert.That(harness.Elements().Any(e => e.Attribute("role") == "menu"), Is.False);
        });
    }

    [Test]
    public async Task KeyDown_escapeInsideTheOverflowMenu_closesIt()
    {
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", isOverflowOpen: true, overflowStates: overflowStates));

        await harness.KeyDownAsync(e => e.HasClass("lx-tabstrip-overflow"), "Escape");

        Assert.That(overflowStates, Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task KeyDown_anyOtherKeyInsideTheOverflowMenu_leavesItOpen()
    {
        var overflowStates = new List<bool>();
        await using var harness = await DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(
            Parameters(LatticeBreakpoint.Compact, "metrics", isOverflowOpen: true, overflowStates: overflowStates));

        await harness.KeyDownAsync(e => e.HasClass("lx-tabstrip-overflow"), "ArrowDown");

        Assert.That(overflowStates, Is.Empty);
    }
}
