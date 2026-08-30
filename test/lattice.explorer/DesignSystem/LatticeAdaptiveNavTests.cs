using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the adaptive navigation primitive, exercised at every
/// breakpoint: a persistent sidebar at expanded, a dismissible drawer at
/// medium, and a bottom bar with an overflow menu at compact.
/// </summary>
[TestFixture]
public sealed class LatticeAdaptiveNavTests
{
    private static readonly LatticeNavItem[] SixDestinations =
    [
        new("explore", "Explore"),
        new("backups", "Backups"),
        new("access", "Access"),
        new("schema", "Schema"),
        new("tenants", "Tenants"),
        new("my-tenant", "My tenant"),
    ];

    private static Task<string> RenderAsync(
        LatticeBreakpoint breakpoint,
        IReadOnlyList<LatticeNavItem>? items = null,
        string? selectedId = null,
        bool isDrawerOpen = false) =>
        DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(new Dictionary<string, object?>
        {
            ["Breakpoint"] = breakpoint,
            ["Items"] = items ?? SixDestinations,
            ["SelectedId"] = selectedId,
            ["IsDrawerOpen"] = isDrawerOpen,
            ["Id"] = "nav",
        });

    // ---------------------------------------------------------------- expanded

    [Test]
    public async Task Render_atExpanded_isAPersistentSidebar()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-sidebar"));
            Assert.That(html, Does.Contain("data-lx-breakpoint=\"expanded\""));
            Assert.That(html, Does.Not.Contain("lx-nav-drawer"));
            Assert.That(html, Does.Not.Contain("lx-nav-bottom"));
        });
    }

    [Test]
    public async Task Render_atExpanded_showsEveryDestinationWithNoOverflow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            foreach (var item in SixDestinations)
            {
                Assert.That(html, Does.Contain($">{item.Label}</button>"), $"{item.Label} must be listed");
            }

            Assert.That(html, Does.Not.Contain("lx-nav-overflow"));
        });
    }

    [Test]
    public async Task Render_atExpanded_marksTheSelectedDestinationAsCurrent()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, selectedId: "access");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-current=\"page\""));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-current=\"page\""),
                Is.EqualTo(1),
                "exactly one destination is current");
            Assert.That(html, Does.Contain("lx-nav-item is-selected"));
        });
    }

    [Test]
    public async Task Render_withNoSelection_marksNothingAsCurrent()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded);

        Assert.That(html, Does.Not.Contain("aria-current"));
    }

    [Test]
    public async Task Render_withAnUnknownSelection_marksNothingAsCurrent()
    {
        var html = await RenderAsync(LatticeBreakpoint.Expanded, selectedId: "not-a-destination");

        Assert.That(html, Does.Not.Contain("aria-current"));
    }

    // ------------------------------------------------------------------ medium

    [Test]
    public async Task Render_atMedium_isAClosedDismissibleDrawerBehindAToggle()
    {
        var html = await RenderAsync(LatticeBreakpoint.Medium);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-drawer-toggle"));
            Assert.That(html, Does.Contain("aria-expanded=\"false\""));
            Assert.That(html, Does.Contain("aria-controls=\"nav-drawer\""));
            Assert.That(html, Does.Not.Contain("lx-nav-scrim"), "a closed drawer renders no scrim");
            Assert.That(html, Does.Not.Contain("lx-nav-sidebar"));
        });
    }

    [Test]
    public async Task Render_atMediumWithTheDrawerClosed_keepsTheDestinationsOutOfTheDom()
    {
        var html = await RenderAsync(LatticeBreakpoint.Medium);

        Assert.That(html, Does.Not.Contain("lx-nav-list"),
            "a closed drawer must not leave focusable destinations in the tab order");
    }

    [Test]
    public async Task Render_atMediumWithTheDrawerOpen_showsTheDrawerScrimAndDestinations()
    {
        var html = await RenderAsync(LatticeBreakpoint.Medium, isDrawerOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-expanded=\"true\""));
            Assert.That(html, Does.Contain("lx-nav-scrim"));
            Assert.That(html, Does.Contain("id=\"nav-drawer\""));
            Assert.That(html, Does.Contain("lx-nav-drawer-close"));
            Assert.That(html, Does.Contain("Close navigation"));

            foreach (var item in SixDestinations)
            {
                Assert.That(html, Does.Contain($">{item.Label}</button>"));
            }
        });
    }

    [Test]
    public async Task Render_atMedium_neverCollapsesTheDestinationsIntoAnOverflow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Medium, isDrawerOpen: true);

        Assert.That(html, Does.Not.Contain("lx-nav-overflow"),
            "the drawer scrolls, so it renders every destination");
    }

    // ----------------------------------------------------------------- compact

    [Test]
    public async Task Render_atCompact_isABottomBar()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-bottom"));
            Assert.That(html, Does.Contain("data-lx-breakpoint=\"compact\""));
            Assert.That(html, Does.Contain("lx-nav-bar-list"));
            Assert.That(html, Does.Not.Contain("lx-nav-sidebar"));
            Assert.That(html, Does.Not.Contain("lx-nav-drawer"));
        });
    }

    [Test]
    public async Task Render_atCompact_keepsOnlyTheBarCapacityInlineAndOffersAnOverflow()
    {
        var html = await RenderAsync(LatticeBreakpoint.Compact);

        var inlineButtons = DesignSystemRenderHarness.CountOccurrences(html, "class=\"lx-nav-bar-item");
        var expected = LatticeBreakpoints.CompactNavigationInlineCapacity + 1; // destinations plus the overflow toggle

        Assert.Multiple(() =>
        {
            Assert.That(inlineButtons, Is.EqualTo(expected));
            Assert.That(html, Does.Contain("lx-nav-overflow-toggle"));
            Assert.That(html, Does.Contain("aria-haspopup=\"menu\""));
            Assert.That(html, Does.Contain("aria-controls=\"nav-overflow\""));
            Assert.That(html, Does.Contain(">More</button>"));
        });
    }

    [Test]
    public async Task Render_atCompact_withFewDestinations_rendersNoOverflowControl()
    {
        var html = await RenderAsync(
            LatticeBreakpoint.Compact,
            items: SixDestinations.Take(LatticeBreakpoints.CompactNavigationInlineCapacity).ToArray());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("lx-nav-overflow-toggle"));
            Assert.That(html, Does.Contain("lx-nav-bar-list"));
        });
    }

    [Test]
    public async Task Render_atCompact_keepsTheSelectedDestinationVisibleEvenWhenItOverflows()
    {
        // "my-tenant" is the last of six, well beyond the compact bar's capacity.
        var html = await RenderAsync(LatticeBreakpoint.Compact, selectedId: "my-tenant");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-bar-item is-selected"));
            Assert.That(html, Does.Contain(">My tenant</button>"));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-current=\"page\""),
                Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Render_atCompact_usesTheShortLabelInTheBar()
    {
        LatticeNavItem[] items =
        [
            new("dead-letter", "Dead letters") { ShortLabel = "DLQ" },
            new("data", "Data"),
        ];

        var html = await RenderAsync(LatticeBreakpoint.Compact, items: items);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">DLQ</button>"));
            Assert.That(html, Does.Not.Contain(">Dead letters</button>"));
        });
    }

    [Test]
    public async Task Render_atExpanded_usesTheFullLabelEvenWhenAShortLabelExists()
    {
        LatticeNavItem[] items = [new("dead-letter", "Dead letters") { ShortLabel = "DLQ" }];

        var html = await RenderAsync(LatticeBreakpoint.Expanded, items: items);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Dead letters</button>"));
            Assert.That(html, Does.Not.Contain(">DLQ</button>"));
        });
    }

    // ----------------------------------------------------- gating and a11y

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_showsADeniedDestinationDisabledRatherThanHidden(
        LatticeBreakpoint breakpoint)
    {
        LatticeNavItem[] items =
        [
            new("explore", "Explore"),
            new("access", "Access") { IsEnabled = false, Description = "Not available for your account." },
        ];

        var html = await RenderAsync(breakpoint, items: items, isDrawerOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Access"), "a denied destination stays visible");
            Assert.That(html, Does.Contain("disabled"));
            Assert.That(html, Does.Contain("Not available for your account."));
        });
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_exposesANamedNavigationLandmark(LatticeBreakpoint breakpoint)
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = breakpoint,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["Label"] = "Explorer areas",
                ["Id"] = "nav",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<nav"));
            Assert.That(html, Does.Contain("aria-label=\"Explorer areas\""));
        });
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_atEveryBreakpoint_makesEveryDestinationAKeyboardReachableButton(
        LatticeBreakpoint breakpoint)
    {
        var html = await RenderAsync(breakpoint, isDrawerOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("type=\"button\""));
            Assert.That(html, Does.Not.Contain("tabindex=\"-1\""),
                "navigation destinations are ordinary tab stops");
        });
    }

    // -------------------------------------------------------------- defaults

    [Test]
    public async Task Render_withNoBreakpoint_fallsBackToTheDefaultShape()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?> { ["Items"] = SixDestinations });

        Assert.That(html, Does.Contain($"data-lx-breakpoint=\"{LatticeBreakpoints.Name(LatticeBreakpoints.Default)}\""));
    }

    [Test]
    public async Task Render_followsTheCascadedShellContextWhenNoBreakpointIsPinned()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveNav>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?> { ["Items"] = SixDestinations });

        Assert.That(html, Does.Contain("lx-nav-bottom"));
    }

    [Test]
    public async Task Render_pinnedBreakpointWinsOverTheCascadedShellContext()
    {
        var html = await DesignSystemRenderHarness.RenderCascadedAsync<LatticeAdaptiveNav>(
            new LatticeAdaptiveContext(LatticeBreakpoint.Compact, LatticeDensity.Cosy, IsMeasured: true),
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = (LatticeBreakpoint?)LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-sidebar"));
            Assert.That(html, Does.Not.Contain("lx-nav-bottom"));
        });
    }

    [Test]
    public async Task Render_atCompact_theOpenOverflowMenuListsEveryDestination()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["SelectedId"] = "explore",
                ["IsOverflowOpen"] = true,
                ["Id"] = "nav",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"menu\""));
            Assert.That(html, Does.Contain("aria-labelledby=\"nav-overflow-toggle\""));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "role=\"menuitemradio\""),
                Is.EqualTo(SixDestinations.Length));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "aria-checked=\"true\""),
                Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Render_anOverflowOpenedAtCompactIsClosedAtAWiderBreakpoint()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
                ["IsOverflowOpen"] = true,
                ["Id"] = "nav",
            });

        Assert.That(html, Does.Not.Contain("role=\"menu\""));
    }

    [Test]
    public async Task Render_appendsTheCallersClassToTheNavigationElement()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
                ["Class"] = "explorer-nav-host",
            });

        Assert.That(html, Does.Contain("lx-nav lx-nav-sidebar explorer-nav-host"));
    }

    [TestCase(LatticeBreakpoint.Compact)]
    [TestCase(LatticeBreakpoint.Medium)]
    [TestCase(LatticeBreakpoint.Expanded)]
    public async Task Render_withNoDestinations_showsTheEmptyStateAndNoDestinationButtons(
        LatticeBreakpoint breakpoint)
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = breakpoint,
                ["Items"] = Array.Empty<LatticeNavItem>(),
                ["IsDrawerOpen"] = true,
                ["EmptyContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<p>No areas.</p>")),
                ["Id"] = "nav",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("No areas."));
            Assert.That(html, Does.Not.Contain("lx-nav-item"));
            Assert.That(html, Does.Not.Contain("lx-nav-overflow-toggle"));
        });
    }

    [Test]
    public async Task Render_withNullDestinations_doesNotThrow()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?> { ["Breakpoint"] = LatticeBreakpoint.Expanded });

        Assert.That(html, Does.Contain("lx-nav-sidebar"));
    }

    [Test]
    public async Task Render_atExpanded_placesTheHeaderAndFooterAroundTheDestinationList()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Expanded,
                ["Items"] = SixDestinations,
                ["HeaderContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<div>filter</div>")),
                ["FooterContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<div>status</div>")),
            });

        var header = html.IndexOf("filter", StringComparison.Ordinal);
        var list = html.IndexOf("lx-nav-list", StringComparison.Ordinal);
        var footer = html.IndexOf("status", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(header, Is.GreaterThanOrEqualTo(0));
            Assert.That(header, Is.LessThan(list));
            Assert.That(list, Is.LessThan(footer));
        });
    }

    [Test]
    public async Task Render_atCompact_omitsTheHeaderAndFooterTheBarHasNoRoomFor()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["HeaderContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<div>filter</div>")),
                ["FooterContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<div>status</div>")),
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("filter"));
            Assert.That(html, Does.Not.Contain("status"));
        });
    }

    [Test]
    public async Task Render_atMedium_usesTheCallersDrawerLabels()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Medium,
                ["Items"] = SixDestinations,
                ["IsDrawerOpen"] = true,
                ["DrawerToggleLabel"] = "Areas",
                ["DrawerCloseLabel"] = "Dismiss areas",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Areas</button>"));
            Assert.That(html, Does.Contain("Dismiss areas"));
        });
    }

    [Test]
    public async Task Render_atCompact_usesTheCallersOverflowLabel()
    {
        var html = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(
            new Dictionary<string, object?>
            {
                ["Breakpoint"] = LatticeBreakpoint.Compact,
                ["Items"] = SixDestinations,
                ["OverflowLabel"] = "All areas",
            });

        Assert.That(html, Does.Contain(">All areas</button>"));
    }

    [Test]
    public async Task Render_generatesADistinctElementIdPerInstanceWhenNoneIsSupplied()
    {
        var parameters = new Dictionary<string, object?>
        {
            ["Breakpoint"] = LatticeBreakpoint.Medium,
            ["Items"] = SixDestinations,
        };

        var first = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(parameters);
        var second = await DesignSystemRenderHarness.RenderAsync<LatticeAdaptiveNav>(parameters);

        Assert.Multiple(() =>
        {
            Assert.That(first, Does.Contain("aria-controls=\"lx-nav-"));
            Assert.That(first, Is.Not.EqualTo(second), "each instance gets its own element ids");
        });
    }
}
