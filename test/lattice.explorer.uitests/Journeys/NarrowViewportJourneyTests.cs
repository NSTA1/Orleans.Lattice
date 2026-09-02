using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// <b>Journey: narrow viewport.</b> At 320px - the narrowest viewport the product
/// supports - and right across the compact band, every area must still be reachable,
/// the catalog must still be openable, and anything that spilled into an overflow menu
/// must be wholly on screen.
/// </summary>
/// <remarks>
/// The rail is vertical and scrolls rather than collapsing, so at a phone width the
/// question is not "can the areas fit" but "is anything the layout moved still
/// reachable". The per-selection strip is what genuinely overflows there, which is why
/// this journey opens a tree first.
/// </remarks>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class NarrowViewportJourneyTests : JourneyTestBase
{
    /// <summary>
    /// Widths spanning the compact band and its boundary into medium. Sampled by
    /// resizing one page rather than reloading at each width: the breakpoint observer
    /// reports a resize, so the strip re-measures, and the journey stays fast enough to
    /// live in the lane.
    /// </summary>
    private static readonly int[] CompactBandWidths = [320, 360, 390, 430, 480, 540, 599];

    [Test]
    public async Task Every_area_is_reachable_at_the_narrowest_supported_viewport()
    {
        var page = await OpenAtAsync("", NarrowWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await ExplorerShell.AssertBreakpointAsync(page, LatticeBreakpoint.Compact);

        // Enumerating the rail before its gates have reported reads a set that is
        // still changing: an area shown plainly while unprobed can demote a moment
        // later, so a tab counted here need not be the tab clicked below.
        await JourneyShell.AssertRailSettledAsync(page);

        var tabs = page.Locator(JourneyShell.RailTabSelector);
        var count = await tabs.CountAsync();

        Assert.That(count, Is.GreaterThan(1),
            $"The rail offered {count} area(s) at {NarrowWidth}px, so 'every area is reachable' would "
            + "be indistinguishable from 'there is only one area'.");

        // Every offered area must actually open, not merely be present in the DOM: at a
        // phone width an area can be rendered but positioned outside the scroll extent.
        for (var i = 0; i < count; i++)
        {
            var label = (await tabs.Nth(i).TextContentAsync() ?? string.Empty).Trim();
            await JourneyShell.OpenAreaAsync(page, label);
        }
    }

    [Test]
    public async Task The_catalog_is_reachable_at_the_narrowest_supported_viewport()
    {
        var page = await OpenAtAsync("", NarrowWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await ExplorerShell.AssertBreakpointAsync(page, LatticeBreakpoint.Compact);

        // Compact puts the catalog behind a drawer so the detail surface gets the width.
        // The drawer must therefore name and control what it opens, or it is a button
        // that visibly does something and announces nothing.
        var toggle = page.Locator(JourneyShell.CatalogDrawerToggleSelector);
        await Assertions.Expect(toggle).ToBeVisibleAsync();
        await Assertions.Expect(toggle).ToHaveAttributeAsync("aria-expanded", "false");
        await Assertions.Expect(toggle).ToHaveAttributeAsync("aria-controls", new Regex(".+"));

        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);
        await Assertions
            .Expect(page.Locator(JourneyShell.DetailStripSelector + " [role=tab][aria-selected='true']"))
            .ToHaveTextAsync("Data");
    }

    [Test]
    public async Task The_overflow_menu_is_fully_on_screen_across_the_compact_band()
    {
        var page = await OpenAtAsync("", NarrowWidth);
        await ExplorerShell.SignInAsync(page, JourneyWorld.PlatformAdmin);
        await JourneyShell.OpenCatalogItemAsync(page, JourneyCatalogReader.OrdersTree);

        // Settle both strips before measuring overflow: an unprobed area or surface is
        // rendered enabled and takes space until its gate reports, so the tab counts - and
        // therefore whether anything overflows - are still moving until then. The toggle
        // selector spans every strip, so the rail matters here as much as the detail one.
        await JourneyShell.AssertRailSettledAsync(page);
        await JourneyShell.AssertDetailStripSettledAsync(page);

        var measured = new List<string>();
        var clipped = new List<string>();

        foreach (var width in CompactBandWidths)
        {
            await page.SetViewportSizeAsync(width, Height);

            // Wait on the strip the resize re-measures rather than on a delay, so this
            // stays web-first at every width.
            await Assertions.Expect(page.Locator(JourneyShell.DetailTabSelector).First).ToBeVisibleAsync();

            // One decision rather than two. Counting the toggle and then opening it in a
            // separate call leaves a window for the toggle to disappear between the two,
            // which is a real race here because a strip stops rendering its toggle the
            // moment it stops overflowing.
            var geometry = await JourneyShell.TryMeasureOverflowMenuAsync(page);
            if (geometry is null)
            {
                continue;
            }

            measured.Add($"{width}px: {geometry}");

            if (!geometry.IsContained)
            {
                clipped.Add($"{width}px: {geometry}");
            }
        }

        Assert.That(measured, Is.Not.Empty,
            "No overflow menu appeared at any width in the compact band, so this case measured "
            + "nothing. The per-selection strip is expected to overflow at a phone width; if it no "
            + "longer does, this journey needs a wider strip rather than a weaker assertion.");

        Assert.That(clipped, Is.Empty,
            "An overflow menu fell outside the viewport, so part of it is unreachable. The audit "
            + "measured exactly this, clipped by a constant 25.2px right across the compact band."
            + Environment.NewLine + "Clipped:" + Environment.NewLine
            + string.Join(Environment.NewLine, clipped)
            + Environment.NewLine + "All measured:" + Environment.NewLine
            + string.Join(Environment.NewLine, measured));
    }
}

