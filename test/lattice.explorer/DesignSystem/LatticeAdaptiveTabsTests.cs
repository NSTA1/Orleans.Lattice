using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the adaptive tab strip, exercised at every breakpoint.
/// The behaviour under test is the one the Explorer's fixed strip lacks: tabs
/// that do not fit move into an overflow menu instead of scrolling off-screen,
/// and the active tab is always visible.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveTabsTests
{
    private static readonly LatticeTabItem[] SixTabs =
    [
        new("metrics", "Metrics"),
        new("topology", "Topology"),
        new("data", "Data"),
        new("dead-letter", "Dead letters"),
        new("history", "History"),
        new("tag-index", "Tag index"),
    ];

    private static Task<string> RenderAsync(
        LatticeBreakpoint breakpoint,
        IReadOnlyList<LatticeTabItem>? tabs = null,
        string? activeId = null,
        int? inlineCapacity = null) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(new Dictionary<string, object?>
        {
            ["Breakpoint"] = breakpoint,
            ["Tabs"] = tabs ?? SixTabs,
            ["ActiveId"] = activeId,
            ["InlineCapacity"] = inlineCapacity,
            ["Id"] = "tabs",
        });

    private static int CountInlineTabs(string html) =>
        DesignSystemRenderHarness.CountOccurrences(html, "role=\"tab\"");

    // ----------------------------------------------------- tablist semantics

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_keepsTheTablistSemantics(LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint, activeId: "metrics");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(html, Does.Contain("role=\"tab\""));
            Assert.That(html, Does.Contain("aria-selected=\"true\""));
            Assert.That(html, Does.Contain("aria-orientation=\"horizontal\""));
        });
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_selectsExactlyOneTab(LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint, activeId: "data");

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(html, "aria-selected=\"true\""),
            Is.EqualTo(1));
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_usesARovingTabindexSoTheStripIsOneTabStop(
        LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint, activeId: "topology");

        Assert.That(
            DesignSystemRenderHarness.CountOccurrences(html, "tabindex=\"0\""),
            Is.EqualTo(1),
            "exactly one tab carries tabindex=0");
    }

    [Test]
    public async Task Render_withNoActiveTab_putsTheFirstEnabledTabInTheTabOrder()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "tabindex=\"0\""), Is.EqualTo(1));
            Assert.That(html, Does.Not.Contain("aria-selected=\"true\""));
        });
    }

    [Test]
    public async Task Render_withADisabledActiveTab_movesTheTabStopToAnEnabledTab()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics") { IsEnabled = false },
            new("data", "Data"),
        ];

        var html = await RenderAsync(LatticeBreakpoint.Expanded, tabs: tabs, activeId: "metrics");

        Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "tabindex=\"0\""), Is.EqualTo(1));
    }

    [Test]
    public async Task Render_withEveryTabDisabled_placesNoTabInTheTabOrder()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics") { IsEnabled = false },
            new("data", "Data") { IsEnabled = false },
        ];

        var html = await RenderAsync(LatticeBreakpoint.Expanded, tabs: tabs);

        Assert.That(html, Does.Not.Contain("tabindex=\"0\""));
    }

    // ---------------------------------------------------------------- overflow

    [Test]
    public async Task Render_atExpanded_showsEveryTabInlineWhenTheyAllFit()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, activeId: "metrics");

        Assert.Multiple(() =>
        {
            Assert.That(CountInlineTabs(html), Is.EqualTo(SixTabs.Length));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    [Test]
    public async Task Render_atMedium_keepsTheBreakpointsCapacityInlineAndOverflowsTheRest()
    {
        var html = await RenderAsync(LatticeBreakpoint.Medium, activeId: "metrics");

        Assert.Multiple(() =>
        {
            Assert.That(CountInlineTabs(html), Is.EqualTo(LatticeBreakpoints.MediumTabInlineCapacity));
            Assert.That(html, Does.Contain("lx-tabstrip-overflow-toggle"));
            Assert.That(html, Does.Contain("aria-haspopup=\"menu\""));
            Assert.That(html, Does.Contain("aria-controls=\"tabs-overflow\""));
        });
    }

    [Test]
    public async Task Render_atCompact_keepsOnlyTheActiveTabInline()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact, activeId: "history");

        Assert.Multiple(() =>
        {
            Assert.That(CountInlineTabs(html), Is.EqualTo(LatticeBreakpoints.CompactTabInlineCapacity));
            Assert.That(html, Does.Contain(">History</button>"));
            Assert.That(html, Does.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    public async Task Render_atNarrowBreakpoints_keepsTheActiveTabVisibleEvenWhenItWouldOverflow(
        LatticeBreakpoint breakpoint)
    {
        // "tag-index" is the last of six, beyond both narrow capacities.
        var html = await RenderAsync(breakpoint, activeId: "tag-index");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tab is-active"));
            Assert.That(html, Does.Contain("aria-selected=\"true\""));
            Assert.That(html, Does.Contain(">Tag index</button>"));
        });
    }

    [Test]
    public async Task Render_theOpenOverflowMenuListsEveryTabSoNoneScrollsOutOfReach()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
                ["IsOverflowOpen"] = true,
                ["Id"] = "tabs",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"menu\""));
            Assert.That(html, Does.Contain("aria-expanded=\"true\""));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "role=\"menuitemradio\""),
                Is.EqualTo(SixTabs.Length),
                "the menu is a complete picker, so every tab stays reachable");
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-checked=\"true\""),
                Is.EqualTo(1),
                "the menu shows which tab is active");

            foreach (var tab in SixTabs)
            {
                Assert.That(html, Does.Contain($">{tab.Label}</button>"), $"{tab.Label} must be reachable");
            }
        });
    }

    [Test]
    public async Task Render_theOverflowMenuIsLabelledByItsToggle()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
                ["IsOverflowOpen"] = true,
                ["Id"] = "tabs",
            });

        Assert.That(html, Does.Contain("aria-labelledby=\"tabs-overflow-toggle\""));
    }

    [Test]
    public async Task Render_anOverflowOpenedWhileNarrowIsClosedWhenTheStripNoLongerOverflows()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
                ["IsOverflowOpen"] = true,
                ["Id"] = "tabs",
            });

        Assert.That(html, Does.Not.Contain("role=\"menu\""),
            "a menu left open across a breakpoint change would be orphaned");
    }

    [Test]
    public async Task Render_whenClosed_leavesTheOverflowMenuOutOfTheDom()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact, activeId: "metrics");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-expanded=\"false\""));
            Assert.That(html, Does.Not.Contain("role=\"menu\""));
            Assert.That(html, Does.Not.Contain("role=\"menuitemradio\""));
        });
    }

    [Test]
    public async Task Render_anExplicitInlineCapacityOverridesTheBreakpointToken()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, activeId: "metrics", inlineCapacity: 2);

        Assert.Multiple(() =>
        {
            Assert.That(CountInlineTabs(html), Is.EqualTo(2));
            Assert.That(html, Does.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    [Test]
    public async Task Render_aCapacityWiderThanTheStripRendersNoOverflow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact, activeId: "metrics", inlineCapacity: 99);

        Assert.Multiple(() =>
        {
            Assert.That(CountInlineTabs(html), Is.EqualTo(SixTabs.Length));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    // -------------------------------------------------------- gating and panel

    [Test]
    public async Task Render_showsADisabledTabGreyedRatherThanHidden()
    {
        LatticeTabItem[] tabs =
        [
            new("metrics", "Metrics"),
            new("schema", "Schema") { IsEnabled = false, Description = "Schema is not installed." },
        ];

        var html = await RenderAsync(LatticeBreakpoint.Expanded, tabs: tabs, activeId: "metrics");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Schema</button>"));
            Assert.That(html, Does.Contain("disabled"));
            Assert.That(html, Does.Contain("Schema is not installed."));
        });
    }

    [Test]
    public async Task Render_withPanelContent_completesTheTabsPattern()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "data",
                ["Id"] = "tabs",
                ["ChildContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<p>rows</p>")),
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"tabpanel\""));
            Assert.That(html, Does.Contain("id=\"tabs-panel\""));
            Assert.That(html, Does.Contain("aria-controls=\"tabs-panel\""));
            Assert.That(html, Does.Contain("aria-labelledby=\"tabs-tab-data\""));
            Assert.That(html, Does.Contain("<p>rows</p>"));
        });
    }

    [Test]
    public async Task Render_withoutPanelContent_rendersNoPanelAndNoDanglingAriaControls()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, activeId: "data");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("role=\"tabpanel\""));
            Assert.That(html, Does.Not.Contain("aria-controls=\"tabs-panel\""));
        });
    }

    [Test]
    public async Task Render_placesTrailingContentAfterTheStrip()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "data",
                ["TrailingContent"] = (RenderFragment)(builder =>
                    builder.AddMarkupContent(0, "<span>orders</span>")),
            });

        var strip = html.IndexOf("role=\"tablist\"", StringComparison.Ordinal);
        var trailing = html.IndexOf("orders", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(trailing, Is.GreaterThanOrEqualTo(0));
            Assert.That(strip, Is.LessThan(trailing));
        });
    }

    // -------------------------------------------------------------- defaults

    [Test]
    public async Task Render_withNoBreakpoint_fallsBackToTheDefaultShape()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?> { ["Tabs"] = SixTabs, ["ActiveId"] = "metrics" });

        Assert.That(html, Does.Contain($"data-lx-breakpoint=\"{LatticeBreakpoints.Name(LatticeBreakpoints.Default)}\""));
    }

    [Test]
    public async Task Render_followsTheCascadedShellContextWhenNoBreakpointIsPinned()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveTabs>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?>
            {
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("data-lx-breakpoint=\"compact\""));
            Assert.That(CountInlineTabs(html), Is.EqualTo(LatticeBreakpoints.CompactTabInlineCapacity));
        });
    }

    [Test]
    public async Task Render_pinnedBreakpointWinsOverTheCascadedShellContext()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveTabs>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
            });

        Assert.That(html, Does.Contain("data-lx-breakpoint=\"expanded\""));
    }

    [Test]
    public async Task Render_usesTheCallersLabels()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Tabs"] = SixTabs,
                ["ActiveId"] = "metrics",
                ["Label"] = "Detail tabs",
                ["OverflowLabel"] = "All tabs",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-label=\"Detail tabs\""));
            Assert.That(html, Does.Contain(">All tabs</button>"));
        });
    }

    [Test]
    public async Task Render_appendsTheCallersClassToTheHostElement()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Tabs"] = SixTabs,
                ["Class"] = "explorer-detail-header",
            });

        Assert.That(html, Does.Contain("lx-tabstrip-host explorer-detail-header"));
    }

    [Test]
    public async Task Render_withNoTabs_rendersAnEmptyTablistRatherThanThrowing()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?> { ["Tabs"] = Array.Empty<LatticeTabItem>() });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"tablist\""));
            Assert.That(html, Does.Not.Contain("role=\"tab\""));
            Assert.That(html, Does.Not.Contain("lx-tabstrip-overflow-toggle"));
        });
    }

    [Test]
    public async Task Render_withNullTabs_doesNotThrow()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(
            new Dictionary<string, object?> { ["Breakpoint"] = LatticeBreakpoint.Medium });

        Assert.That(html, Does.Contain("role=\"tablist\""));
    }

    [Test]
    public async Task Render_withAnUnknownActiveId_selectsNothing()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, activeId: "not-a-tab");

        Assert.That(html, Does.Not.Contain("aria-selected=\"true\""));
    }

    [Test]
    public async Task Render_generatesADistinctElementIdPerInstanceWhenNoneIsSupplied()
    {
        var parameters = new Dictionary<string, object?>
        {
            ["Breakpoint"] = LatticeBreakpoint.Expanded,
            ["Tabs"] = SixTabs,
            ["ActiveId"] = "metrics",
        };

        var first = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(parameters);
        var second = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveTabs>(parameters);

        Assert.Multiple(() =>
        {
            Assert.That(first, Does.Contain("id=\"lx-tabs-"));
            Assert.That(first, Is.Not.EqualTo(second));
        });
    }
}
