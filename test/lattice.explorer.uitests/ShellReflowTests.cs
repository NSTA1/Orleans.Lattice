using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Viewport-reflow baseline for the Explorer shell. These are the assertions unit
/// tests provably could not make (issue #1792): the shell frame renders only in a
/// real browser, and the compact/expanded decision comes from the design system's
/// <c>matchMedia</c> breakpoint observer reading the actual viewport - which
/// Playwright controls directly and a headless render or a stubbed
/// <c>window.innerWidth</c> cannot.
/// <para>
/// Every assertion is against <b>computed geometry</b> (<c>boundingBox()</c>), not
/// class names alone. #1792 shipped with the correct class and the wrong CSS, so a
/// class-name assertion would not have caught it; only measuring the rendered pane
/// width does.
/// </para>
/// <para>
/// Carries <c>[Category("Integration")]</c> in addition to <c>[Category("UI")]</c>
/// because the suite transitively depends on a running <c>IHost</c> (the in-process
/// Explorer web head), matching the slow-category convention.
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class ShellReflowTests : UiTestBase
{
    // Widths that land squarely inside each breakpoint band (compact < 600 <= medium
    // < 1024 <= expanded), chosen to be representative phone / tablet / desktop sizes.
    private const int CompactWidth = 390;
    private const int MediumWidth = 800;
    private const int ExpandedWidth = 1400;
    private const int Height = 900;

    // The catalog rail is 20rem. At the default 16px root font size that is 320 CSS
    // pixels; allow a small tolerance for borders and sub-pixel rounding.
    private const double ExpectedNavWidth = 320d;
    private const double NavWidthTolerance = 8d;

    [Test]
    public async Task Compact_hides_the_catalog_pane_behind_a_drawer_toggle()
    {
        var page = await OpenHomeAsync(CompactWidth, Height);

        // The layout stacks rather than placing the catalog as a side pane.
        var stackedLayout = page.Locator(".lx-shell-area-content .lx-shell-layout.is-stacked");
        await Assertions.Expect(stackedLayout).ToBeVisibleAsync();

        // The drawer toggle is present in compact.
        var toggle = page.Locator(".lx-nav-drawer-toggle");
        await Assertions.Expect(toggle).ToBeVisibleAsync();

        // The catalog is NOT occupying a fixed side pane: with the drawer closed there
        // is no rendered .lx-shell-nav taking layout width.
        await Assertions.Expect(page.Locator(".lx-shell-nav")).ToHaveCountAsync(0);
    }

    [Test]
    public async Task Compact_drawer_toggle_reveals_the_catalog_drawer()
    {
        var page = await OpenHomeAsync(CompactWidth, Height);

        var toggle = page.Locator(".lx-nav-drawer-toggle");
        await Assertions.Expect(toggle).ToBeVisibleAsync();

        // Opening the drawer reveals the catalog inside an overlay drawer.
        await toggle.ClickAsync();

        var drawer = page.Locator(".lx-nav-drawer");
        await Assertions.Expect(drawer).ToBeVisibleAsync();

        // The catalog pane lives inside the drawer once open.
        await Assertions.Expect(drawer.Locator(".lx-shell-nav")).ToBeVisibleAsync();
    }

    [Test]
    public async Task Medium_renders_the_catalog_as_a_fixed_side_pane()
    {
        await AssertSidePaneLayout(MediumWidth);
    }

    [Test]
    public async Task Expanded_renders_the_catalog_as_a_fixed_side_pane()
    {
        await AssertSidePaneLayout(ExpandedWidth);
    }

    private async Task AssertSidePaneLayout(int width)
    {
        var page = await OpenHomeAsync(width, Height);

        // No drawer toggle above the compact breakpoint.
        await Assertions.Expect(page.Locator(".lx-nav-drawer-toggle")).ToHaveCountAsync(0);

        // The catalog is a real, visible side pane.
        var nav = page.Locator(".lx-shell-nav");
        await Assertions.Expect(nav).ToBeVisibleAsync();

        // Computed geometry: the rail holds its ~320px (20rem) width, and crucially it
        // does NOT swell to eat the viewport (the shape of #1792's regression). This
        // is the assertion a class-name check could not make.
        var box = await nav.BoundingBoxAsync();
        Assert.That(box, Is.Not.Null, "The catalog pane reported no bounding box.");
        Assert.That(box!.Width, Is.EqualTo(ExpectedNavWidth).Within(NavWidthTolerance),
            $"At {width}px the catalog pane should be a fixed ~{ExpectedNavWidth}px (20rem) rail, "
            + $"but measured {box.Width}px.");
    }
}
