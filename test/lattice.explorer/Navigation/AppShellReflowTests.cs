using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell frame's own reflow (issue #1792), and the ARIA state its area strip
/// publishes (issue #1793).
/// </summary>
/// <remarks>
/// <para>
/// These render <c>AppShell</c> itself rather than a plugin inside it, which is
/// the whole point. The epic already had forty compact-reflow assertions and
/// every one of them rendered a plugin, inside a pane whose width the shell
/// never changed - so the frame could hold its desktop two-pane shape at a phone
/// width, clip the detail surface to roughly seventy pixels, and leave the suite
/// entirely green. A shell-level assertion is what closes that.
/// </para>
/// <para>
/// Nothing here waits: every gate answers synchronously and the drawer and
/// overflow states are supplied as parameters, so the markup a given state
/// produces is read from a single settled render.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AppShellReflowTests
{
    private const string StackedLayout = "class=\"lx-shell-layout is-stacked\"";
    private const string PaneLayout = "class=\"lx-shell-layout\"";
    private const string DrawerToggle = "lx-nav-drawer-toggle";
    private const string OverflowToggle = "lx-tabstrip-overflow-toggle";
    private const string Tab = "role=\"tab\"";

    // ---- the frame ----------------------------------------------------------

    [Test]
    public async Task The_catalog_occupies_a_pane_beside_the_detail_surface_at_expanded()
    {
        var html = await AppShellRenderHarness.RenderAsync(LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(PaneLayout), "the desktop frame is a row of two panes");
            Assert.That(html, Does.Not.Contain(StackedLayout));
            Assert.That(
                html,
                Does.Contain("class=\"" + AppShellRenderHarness.CatalogPaneClass + "\""),
                "the catalog keeps the fixed sidebar pane it always had at desktop widths");
            Assert.That(html, Does.Contain(AppShellRenderHarness.CatalogMarker));
            Assert.That(html, Does.Contain(AppShellRenderHarness.DetailMarker));
            Assert.That(html, Does.Not.Contain(DrawerToggle), "and needs no toggle to reach it");
        });
    }

    [Test]
    public async Task The_medium_breakpoint_keeps_the_pane_unchanged()
    {
        var html = await AppShellRenderHarness.RenderAsync(LatticeBreakpoint.Medium);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(PaneLayout));
            Assert.That(html, Does.Not.Contain(StackedLayout));
            Assert.That(html, Does.Contain(AppShellRenderHarness.CatalogMarker));
        });
    }

    [Test]
    public async Task The_catalog_stops_occupying_a_pane_at_compact()
    {
        var html = await AppShellRenderHarness.RenderAsync(LatticeBreakpoint.Compact);

        // This is the assertion the branch could not pass before the fix: at 390
        // pixels the fixed 20rem sidebar held its width and pushed the detail
        // pane off-screen.
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(StackedLayout), "the compact frame stacks rather than splitting");
            Assert.That(html, Does.Not.Contain(PaneLayout));
            Assert.That(
                html,
                Does.Not.Contain(AppShellRenderHarness.CatalogPaneClass),
                "the catalog occupies no pane at all until the caller asks for it");
            Assert.That(html, Does.Not.Contain(AppShellRenderHarness.CatalogMarker));
            Assert.That(html, Does.Contain(DrawerToggle), "it is reached through the drawer toggle instead");
            Assert.That(
                html,
                Does.Contain(AppShellRenderHarness.DetailMarker),
                "so the detail surface gets the whole width");
        });
    }

    [Test]
    public async Task The_compact_catalog_opens_into_the_design_systems_drawer()
    {
        var html = await AppShellRenderHarness.RenderAsync(
            LatticeBreakpoint.Compact,
            isCatalogOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-nav-drawer"), "the drawer is the design system's, not a new shell shape");
            Assert.That(html, Does.Contain("lx-nav-scrim"));
            Assert.That(html, Does.Contain("lx-nav-drawer-close"));
            Assert.That(html, Does.Contain(AppShellRenderHarness.CatalogMarker), "and it carries the catalog");
            Assert.That(
                html,
                Does.Contain(StackedLayout),
                "an overlay, so the frame beneath it is still the stacked one");
        });
    }

    [Test]
    public async Task The_compact_drawer_toggle_is_wired_to_the_drawer_it_controls()
    {
        var html = await AppShellRenderHarness.RenderAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-expanded=\"false\""), "closed is a state, not an absent attribute");
            Assert.That(html, Does.Contain("aria-controls="));
        });
    }

    [Test]
    public async Task An_unmeasured_host_keeps_the_layout_the_explorer_always_shipped()
    {
        // No cascaded context at all: a static render or a head without script
        // must land on the expanded frame, never on the compact one.
        var html = await AppShellRenderHarness.RenderWithoutContextAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(PaneLayout));
            Assert.That(html, Does.Contain(AppShellRenderHarness.CatalogMarker));
        });
    }

    // ---- the area rail ------------------------------------------------------

    [Test]
    public async Task The_rail_renders_every_area_at_every_width_rather_than_overflowing()
    {
        // The horizontal strip this replaced kept one tab inline at compact and
        // pushed the rest into a menu, so a gate that disabled two areas
        // displaced a live one. A column has no such scarcity, and that is the
        // reason the rail is the shape it is.
        var html = await AppShellRenderHarness.RenderAsync(
            LatticeBreakpoint.Compact,
            isCatalogOpen: false,
            AppShellRenderHarness.Plugin("a", "Alpha", 100),
            AppShellRenderHarness.Plugin("b", "Bravo", 200),
            AppShellRenderHarness.Plugin("c", "Charlie", 300));

        Assert.Multiple(() =>
        {
            Assert.That(
                AppShellRenderHarness.CountOccurrences(html, Tab),
                Is.EqualTo(4),
                "the home surface and all three areas, at the narrowest band");
            Assert.That(html, Does.Not.Contain(OverflowToggle), "a rail scrolls rather than overflowing");
        });
    }

    [Test]
    public async Task The_rail_publishes_its_axis_so_the_arrow_keys_match_it()
    {
        var html = await AppShellRenderHarness.RenderAsync(
            LatticeBreakpoint.Expanded,
            isCatalogOpen: false,
            AppShellRenderHarness.Plugin("a", "Alpha", 100));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-orientation=\"vertical\""));
            Assert.That(
                html,
                Does.Contain("class=\"lx-shell-rail lx-shell-areastrip\""),
                "the rail keeps the class the accessibility lane addresses it by");
        });
    }

    [Test]
    public async Task The_rail_is_the_only_tab_list_in_the_shell_frame()
    {
        // Every hand-rolled role=tablist in the shell is gone: the rail, the
        // catalog kind and the detail surfaces all run on the one primitive, and
        // the shell frame itself declares exactly one strip.
        var html = await AppShellRenderHarness.RenderAsync(
            LatticeBreakpoint.Expanded,
            isCatalogOpen: false,
            AppShellRenderHarness.Plugin("a", "Alpha", 100));

        Assert.That(AppShellRenderHarness.CountOccurrences(html, "role=\"tablist\""), Is.EqualTo(1));
    }

    // ---- the ARIA state the strip publishes (issue #1793) -------------------
    //
    // The explicit-aria-selected guarantee is now asserted through the parsed DOM
    // in AppShellAriaSelectedBunitTests. That is the point of the bUnit pattern:
    // a static-markup version had to derive an invalid count by string arithmetic
    // (total minus the two valid spellings) because the naive
    // Does.Not.Contain("aria-selected=\"\"") guard is vacuous - the renderer emits
    // the bare attribute name, so the empty-string form never appears in raw
    // markup, only in a browser DOM after parsing. bUnit reads GetAttribute(...)
    // and sees the browser value, so the assertion catches the bug naturally.

    [Test]
    public async Task The_selected_area_moves_with_the_active_surface()
    {
        // Static markup can only show the pre-activation state; the post-click
        // half lives beside the other transition tests, in AppShellTests, where
        // the render-tree harness can dispatch the activation.
        var html = await AppShellRenderHarness.RenderAsync(
            LatticeBreakpoint.Expanded,
            isCatalogOpen: false,
            AppShellRenderHarness.Plugin("a", "Alpha", 100),
            AppShellRenderHarness.Plugin("b", "Bravo", 200));

        Assert.Multiple(() =>
        {
            Assert.That(
                AppShellRenderHarness.CountOccurrences(html, "aria-selected=\"true\""),
                Is.EqualTo(1),
                "the home surface is the one selected before any activation");
            Assert.That(
                AppShellRenderHarness.CountOccurrences(html, "aria-selected=\"false\""),
                Is.EqualTo(2),
                "and both areas state that they are not");
        });
    }
}
