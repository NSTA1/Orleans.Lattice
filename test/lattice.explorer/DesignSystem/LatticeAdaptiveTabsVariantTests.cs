using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the tab strip's presentation variants and for the parts
/// of the tabs pattern the Explorer's hand-rolled strips lacked:
/// <c>aria-controls</c> pointing at a real panel, and a disabled tab whose
/// explanation is reachable by assistive technology rather than only by hover.
/// </summary>
/// <remarks>
/// The variants exist so the shell's area strip and the catalog-kind toggle can
/// adopt this primitive by naming a shape rather than by re-hand-rolling one.
/// The behaviour under test is therefore that the shape changes and the
/// semantics do not.
/// </remarks>
[TestFixture]
public sealed class LatticeAdaptiveTabsVariantTests
{
    private static readonly LatticeTabItem[] ThreeTabs =
    [
        new("trees", "Trees"),
        new("views", "Views"),
        new("tag-indexes", "Tag indexes"),
    ];

    private static Task<string> RenderAsync(
        LatticeTabsVariant variant,
        IReadOnlyList<LatticeTabItem>? tabs = null,
        string? activeId = "trees",
        string? panelId = null) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(new Dictionary<string, object?>
        {
            ["Breakpoint"] = LatticeBreakpoint.Expanded,
            ["Variant"] = variant,
            ["Tabs"] = tabs ?? ThreeTabs,
            ["ActiveId"] = activeId,
            ["PanelId"] = panelId,
            ["Id"] = "kind",
        });

    // --------------------------------------------------------------- variants

    [Test]
    public async Task Render_theDefaultVariant_isTheUnderlinedTabStrip()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?> { ["Tabs"] = ThreeTabs, ["ActiveId"] = "trees" });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tabstrip-host"));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-segmented"));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-subordinate"));
        });
    }

    [Test]
    public async Task Render_theSegmentedVariant_wearsTheSegmentedTrack()
    {
        var html = await RenderAsync(LatticeTabsVariant.Segmented);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tabstrip-host lx-tabstrip-segmented"));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-subordinate"));
        });
    }

    [Test]
    public async Task Render_theSubordinateVariant_isASegmentedControlRenderedQuieter()
    {
        var html = await RenderAsync(LatticeTabsVariant.Subordinate);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tabstrip-segmented"),
                "the subordinate form derives from the segmented one");
            Assert.That(html, Does.Contain("lx-tabstrip-subordinate"));
        });
    }

    [TestCase(LatticeTabsVariant.Underlined)]
    [TestCase(LatticeTabsVariant.Segmented)]
    [TestCase(LatticeTabsVariant.Subordinate)]
    public async Task Render_everyVariant_keepsTheSameTablistSemantics(LatticeTabsVariant variant)
    {
        var html = await RenderAsync(variant);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(html, Does.Contain("aria-orientation=\"horizontal\""));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "role=\"tab\""),
                Is.EqualTo(ThreeTabs.Length));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-selected=\"true\""),
                Is.EqualTo(1));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "tabindex=\"0\""),
                Is.EqualTo(1),
                "the roving tabindex is the behaviour a hand-rolled strip loses");
        });
    }

    [TestCase(LatticeTabsVariant.Underlined)]
    [TestCase(LatticeTabsVariant.Segmented)]
    [TestCase(LatticeTabsVariant.Subordinate)]
    public async Task Render_everyVariant_marksADisabledTabTheSameWay(LatticeTabsVariant variant)
    {
        LatticeTabItem[] tabs =
        [
            new("trees", "Trees"),
            new("views", "Views") { IsEnabled = false, Description = "Views need a signed-in session." },
        ];

        var html = await RenderAsync(variant, tabs);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Views</button>"), "a denial stays visible");
            Assert.That(html, Does.Contain("disabled"));
        });
    }

    [Test]
    public async Task Render_appendsTheCallersClassAfterTheVariantClasses()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Variant"] = LatticeTabsVariant.Segmented,
                ["Tabs"] = ThreeTabs,
                ["Class"] = "lx-shell-kind-toggle",
            });

        Assert.That(html, Does.Contain("lx-tabstrip-host lx-tabstrip-segmented lx-shell-kind-toggle"));
    }

    [Test]
    public async Task Render_aSegmentedStrip_measuresAgainstTheTighterSegmentGeometry()
    {
        // The segmented geometry is smaller in every dimension, so at a width
        // that overflows the tab geometry it still fits.
        var segmented = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Variant"] = LatticeTabsVariant.Segmented,
                ["Tabs"] = ThreeTabs,
                ["ActiveId"] = "trees",
                ["AvailableWidth"] = (double?)200,
            });

        var underlined = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Variant"] = LatticeTabsVariant.Underlined,
                ["Tabs"] = ThreeTabs,
                ["ActiveId"] = "trees",
                ["AvailableWidth"] = (double?)200,
            });

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(segmented, "role=\"tab\""),
            Is.GreaterThan(DesignSystemRenderHarness.CountOccurrences(underlined, "role=\"tab\"")));
    }

    [Test]
    public async Task Render_anExplicitMetricsOverridesTheVariantsGeometry()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Variant"] = LatticeTabsVariant.Segmented,
                ["Tabs"] = ThreeTabs,
                ["ActiveId"] = "trees",
                ["AvailableWidth"] = (double?)400,

                // A geometry so wide that nothing fits beside the first option.
                ["Metrics"] = (LatticeStripMetrics?)new LatticeStripMetrics(14.4, 200, 4, 64, 0),
            });

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(html, "role=\"tab\""),
            Is.EqualTo(1));
    }

    // -------------------------------------------------------- the tabs pattern

    [Test]
    public async Task Render_withACallerOwnedPanel_pointsEveryTabAtIt()
    {
        var html = await RenderAsync(LatticeTabsVariant.Segmented, panelId: "catalog-list");

        Assert.Multiple(() =>
        {
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-controls=\"catalog-list\""),
                Is.EqualTo(ThreeTabs.Length),
                "every tab names the panel it controls, which is what all three of the "
                + "Explorer's strips lacked");
            Assert.That(html, Does.Not.Contain("role=\"tabpanel\""),
                "the caller renders the panel itself");
        });
    }

    [Test]
    public async Task Render_withOwnPanelContent_prefersItsOwnPanelOverTheNamedOne()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Tabs"] = ThreeTabs,
                ["ActiveId"] = "trees",
                ["Id"] = "kind",
                ["PanelId"] = "somewhere-else",
                ["ChildContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<p>rows</p>")),
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-controls=\"kind-panel\""));
            Assert.That(html, Does.Not.Contain("aria-controls=\"somewhere-else\""));
            Assert.That(html, Does.Contain("id=\"kind-panel\""));
        });
    }

    [Test]
    public async Task Render_withNeitherPanel_omitsAriaControlsRatherThanDanglingIt()
    {
        var html = await RenderAsync(LatticeTabsVariant.Underlined);

        Assert.That(html, Does.Not.Contain("aria-controls=\"kind-panel\""),
            "pointing at an element that does not exist is worse than omitting the attribute");
    }

    // ------------------------------------------------- explaining a disabled tab

    [Test]
    public async Task Render_aTabWithADescription_associatesItForAssistiveTechnology()
    {
        LatticeTabItem[] tabs =
        [
            new("trees", "Trees"),
            new("views", "Views") { IsEnabled = false, Description = "Views need a signed-in session." },
        ];

        var html = await RenderAsync(LatticeTabsVariant.Underlined, tabs);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-describedby=\"kind-tab-views-description\""),
                "a native title is invisible on touch and unreachable by keyboard");
            Assert.That(html, Does.Contain("id=\"kind-tab-views-description\""));
            Assert.That(html, Does.Contain("lx-visually-hidden"));
            Assert.That(html, Does.Contain("Views need a signed-in session."));
        });
    }

    [Test]
    public async Task Render_keepsDescriptionElementsOutOfTheTablist()
    {
        LatticeTabItem[] tabs =
        [
            new("trees", "Trees"),
            new("views", "Views") { IsEnabled = false, Description = "Views need a signed-in session." },
        ];

        var html = await RenderAsync(LatticeTabsVariant.Underlined, tabs);

        var tablistEnd = html.IndexOf("</div>", StringComparison.Ordinal);
        var description = html.IndexOf("id=\"kind-tab-views-description\"", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(description, Is.GreaterThanOrEqualTo(0));
            Assert.That(description, Is.GreaterThan(tablistEnd),
                "a tablist owns tabs; an unroled child among them fails the ARIA "
                + "required-children rule the accessibility lane runs");
        });
    }

    [Test]
    public async Task Render_theOverflowMenu_describesADeniedEntryToo()
    {
        LatticeTabItem[] tabs =
        [
            new("trees", "Trees"),
            new("views", "Views"),
            new("tag-indexes", "Tag indexes") { IsEnabled = false, Description = "Tag indexes are off." },
        ];

        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Tabs"] = tabs,
                ["ActiveId"] = "trees",
                ["InlineCapacity"] = 1,
                ["IsOverflowOpen"] = true,
                ["Id"] = "kind",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"menuitemradio\""));
            Assert.That(html, Does.Contain("aria-describedby=\"kind-tab-tag-indexes-description\""),
                "a denial reached through the overflow menu is explained the same way as "
                + "one reached inline");
            Assert.That(html, Does.Contain("Tag indexes are off."));
        });
    }

    [Test]
    public async Task Render_aTabWithNoDescription_carriesNoDescriptionElement()
    {
        var html = await RenderAsync(LatticeTabsVariant.Underlined);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("aria-describedby"));
            Assert.That(html, Does.Not.Contain("-description\""));
            Assert.That(html, Does.Not.Contain("title="),
                "a label repeated as a tooltip is noise, not an explanation");
        });
    }
}
