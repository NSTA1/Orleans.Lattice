using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component and interaction tests for the vertical tab strip - the rail shape.
/// </summary>
/// <remarks>
/// The WAI-ARIA tabs pattern binds the arrow pair to the strip's axis, so a
/// vertical strip that still moved on Left and Right would both mis-declare
/// itself through <c>aria-orientation</c> and swallow the keys the page scrolls
/// with. These assert the axis end to end: the declared orientation, the keys
/// that move, the keys that do not, and the fact that a rail scrolls rather
/// than overflowing.
/// </remarks>
[TestFixture]
public sealed class LatticeAdaptiveTabsOrientationTests
{
    private static readonly LatticeTabItem[] SixTabs =
    [
        new("explore", "Explore"),
        new("backups", "Backups"),
        new("access", "Access"),
        new("tenants", "Tenants"),
        new("my-tenant", "My tenant"),
        new("telemetry", "Telemetry"),
    ];

    private static Task<string> RenderAsync(
        LatticeTabsOrientation orientation,
        LatticeTabsVariant variant = LatticeTabsVariant.Underlined,
        LatticeBreakpoint breakpoint = LatticeBreakpoint.Compact) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(new Dictionary<string, object?>
        {
            ["Breakpoint"] = breakpoint,
            ["Orientation"] = orientation,
            ["Variant"] = variant,
            ["Tabs"] = SixTabs,
            ["ActiveId"] = "explore",
            ["Id"] = "rail",
        });

    private static Task<DesignSystemInteractiveHarness> RenderInteractiveAsync(
        LatticeTabsOrientation orientation,
        EventCallback<string> onSelect) =>
        DesignSystemInteractiveHarness.RenderAsync<LatticeAdaptiveTabs>(new Dictionary<string, object?>
        {
            ["Breakpoint"] = LatticeBreakpoint.Expanded,
            ["Orientation"] = orientation,
            ["Tabs"] = SixTabs,
            ["ActiveId"] = "backups",
            ["OnSelect"] = onSelect,
        });

    private static bool IsTablist(DesignSystemInteractiveHarness.RenderedElement element) =>
        string.Equals(element.Attribute("role"), "tablist", StringComparison.Ordinal);

    // ------------------------------------------------------------ declaration

    [Test]
    public async Task Render_theDefaultOrientation_isHorizontal()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?> { ["Tabs"] = SixTabs, ["ActiveId"] = "explore" });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-orientation=\"horizontal\""));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-vertical"));
        });
    }

    [Test]
    public async Task Render_aVerticalStrip_declaresItsAxis()
    {
        var html = await RenderAsync(LatticeTabsOrientation.Vertical);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-orientation=\"vertical\""));
            Assert.That(html, Does.Contain("lx-tabstrip-host lx-tabstrip-vertical"));
        });
    }

    [TestCase(LatticeTabsVariant.Underlined)]
    [TestCase(LatticeTabsVariant.Segmented)]
    [TestCase(LatticeTabsVariant.Subordinate)]
    public async Task Render_everyVariant_canRunVertically(LatticeTabsVariant variant)
    {
        var html = await RenderAsync(LatticeTabsOrientation.Vertical, variant);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tabstrip-vertical"));
            Assert.That(html, Does.Contain("aria-orientation=\"vertical\""));
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "tabindex=\"0\""),
                Is.EqualTo(1),
                "the roving tabindex does not depend on the axis");
        });
    }

    // --------------------------------------------------------------- overflow

    [Test]
    public async Task Render_aVerticalStrip_scrollsRatherThanOverflowing()
    {
        // The same six tabs at the same narrow band overflow when horizontal.
        var vertical = await RenderAsync(LatticeTabsOrientation.Vertical);
        var horizontal = await RenderAsync(LatticeTabsOrientation.Horizontal);

        Assert.Multiple(() =>
        {
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(vertical, "role=\"tab\""),
                Is.EqualTo(SixTabs.Length));
            Assert.That(vertical, Does.Not.Contain("lx-tabstrip-overflow-toggle"));
            Assert.That(horizontal, Does.Contain("lx-tabstrip-overflow-toggle"),
                "the horizontal comparison must actually overflow, or this proves nothing");
        });
    }

    [Test]
    public async Task Render_aVerticalStrip_stillHonoursAPinnedCapacity()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Orientation"] = LatticeTabsOrientation.Vertical,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "explore",
                ["InlineCapacity"] = 2,
            });

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "role=\"tab\""), Is.EqualTo(2));
            Assert.That(html, Does.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    // --------------------------------------------------------------- keyboard

    [Test]
    public async Task ArrowDown_movesToTheNextTabInAVerticalStrip()
    {
        var selected = new List<string>();
        await using var harness = await RenderInteractiveAsync(
            LatticeTabsOrientation.Vertical,
            EventCallback.Factory.Create<string>(new object(), selected.Add));

        await harness.KeyDownAsync(IsTablist, "ArrowDown");

        Assert.That(selected, Is.EqualTo(new[] { "access" }));
    }

    [Test]
    public async Task ArrowUp_movesToThePreviousTabInAVerticalStrip()
    {
        var selected = new List<string>();
        await using var harness = await RenderInteractiveAsync(
            LatticeTabsOrientation.Vertical,
            EventCallback.Factory.Create<string>(new object(), selected.Add));

        await harness.KeyDownAsync(IsTablist, "ArrowUp");

        Assert.That(selected, Is.EqualTo(new[] { "explore" }));
    }

    [TestCase("ArrowRight")]
    [TestCase("ArrowLeft")]
    public async Task TheHorizontalArrows_doNothingInAVerticalStrip(string key)
    {
        var selected = new List<string>();
        await using var harness = await RenderInteractiveAsync(
            LatticeTabsOrientation.Vertical,
            EventCallback.Factory.Create<string>(new object(), selected.Add));

        await harness.KeyDownAsync(IsTablist, key);

        Assert.That(selected, Is.Empty,
            "a vertical strip must leave the horizontal arrows to the page");
    }

    [TestCase("ArrowUp")]
    [TestCase("ArrowDown")]
    public async Task TheVerticalArrows_doNothingInAHorizontalStrip(string key)
    {
        var selected = new List<string>();
        await using var harness = await RenderInteractiveAsync(
            LatticeTabsOrientation.Horizontal,
            EventCallback.Factory.Create<string>(new object(), selected.Add));

        await harness.KeyDownAsync(IsTablist, key);

        Assert.That(selected, Is.Empty,
            "a horizontal strip must leave the vertical arrows to the page's scroll");
    }

    [TestCase(LatticeTabsOrientation.Horizontal)]
    [TestCase(LatticeTabsOrientation.Vertical)]
    public async Task HomeAndEnd_moveToTheEndsInEitherAxis(LatticeTabsOrientation orientation)
    {
        var selected = new List<string>();
        await using var harness = await RenderInteractiveAsync(
            orientation,
            EventCallback.Factory.Create<string>(new object(), selected.Add));

        await harness.KeyDownAsync(IsTablist, "Home");
        await harness.KeyDownAsync(IsTablist, "End");

        Assert.That(selected, Is.EqualTo(new[] { "explore", "telemetry" }));
    }
}
